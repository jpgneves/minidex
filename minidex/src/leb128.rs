pub(crate) struct DeltaLeb128Iterator<'a> {
    data: &'a [u8],
    current_doc_id: u32,
    offset: usize,
}

impl<'a> DeltaLeb128Iterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            current_doc_id: 0,
            offset: 0,
        }
    }
}

impl Iterator for DeltaLeb128Iterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }

        let mut delta = 0u32;
        let mut shift = 0;

        while self.offset < self.data.len() {
            let byte = self.data[self.offset];
            self.offset += 1;

            delta += ((byte & 0x7F) as u32) << shift;

            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        self.current_doc_id += delta;
        Some(self.current_doc_id)
    }
}
