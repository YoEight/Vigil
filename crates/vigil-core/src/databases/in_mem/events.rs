use crate::types::Event;

pub struct IndexedEvents<'a, I> {
    indexes: I,
    events: &'a [Event],
}

impl<'a, I> IndexedEvents<'a, I> {
    pub fn new(indexes: I, events: &'a [Event]) -> Self {
        Self { indexes, events }
    }
}

impl<'a, I> Iterator for IndexedEvents<'a, I>
where
    I: Iterator<Item = usize> + 'a,
{
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.indexes.next()?;
        self.events.get(idx)
    }
}
