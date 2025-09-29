use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    ops::{Index, IndexMut},
    rc::Rc,
};

use crate::disk::{DiskManager, PageId, PAGE_SIZE};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BufferId(usize);
pub type Page = [u8; PAGE_SIZE];

pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

pub struct Frame {
    usage_count: u64,
    buffer: Rc<Buffer>,
}

pub struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

impl BufferPool {
    fn size(&self) -> usize {
        self.buffers.len()
    }
    /// 捨てるバッファを決定する
    /// Clock-sweep アルゴリズムを使用
    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;

        // バッファを巡回しながら捨てるバッファを決定する．
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            // バッファの利用回数が0のもの
            if frame.usage_count == 0 {
                break self.next_victim_id;
            }

            // バッファが貸出中ではないか
            if Rc::get_mut(&mut frame.buffer).is_some() {
                // 貸出中でなかったら，そのバッファの利用回数を減らす
                frame.usage_count -= 1;
                // 連続貸出中カウントをリセット
                consecutive_pinned = 0;
            } else {
                // 貸出中だったら連続貸出中カウントを増やす
                consecutive_pinned += 1;
                // 連続貸出中カウントがプールサイズ以上になったら，すべてのバッファが貸出中である．
                // ので，捨てるバッファがないことを示すために None を返す
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };

        todo!()
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        BufferId((buffer_id.0 + 1) % self.size())
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;

    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}

pub struct BufferPoolManager {
    disk: DiskManager,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,
}

impl BufferPoolManager {
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        // すでにページがバッファプールにある
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }

        // ページがバッファプールにない

        // 空きバッファ探し・捨てるバッファを決定
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;

        {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            // dirtyフラグが立っていたら，ディスクに書き出す
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }
            buffer.page_id = page_id;
            // 書き出したからdirtyフラグを下ろす
            buffer.is_dirty.set(false);

            // ページを読み込む
            self.disk.read_page_data(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }

        let page = Rc::clone(&frame.buffer);
        // 捨てたページをページテーブルから削除
        // 読んだページをページテーブルに登録
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);

        Ok(page)
    }
}
