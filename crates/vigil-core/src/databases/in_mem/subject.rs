use std::{
    collections::{HashMap, VecDeque},
    str::Split,
};

#[derive(Default)]
pub struct Subject {
    name: String,
    events: Vec<usize>,
    nodes: HashMap<String, Subject>,
}

impl Subject {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn events(&self) -> &[usize] {
        self.events.as_slice()
    }

    pub fn entries<'a>(&mut self, mut path: impl Iterator<Item = &'a str>) -> &mut Vec<usize> {
        let name = path.next().unwrap_or_default();

        if !name.is_empty() {
            return self
                .nodes
                .entry(name.to_owned())
                .or_insert_with(|| {
                    let name = if self.name.is_empty() {
                        name.to_owned()
                    } else {
                        format!("{}/{}", self.name, name)
                    };

                    Self {
                        name,
                        ..Default::default()
                    }
                })
                .entries(path);
        }

        &mut self.events
    }
}

pub enum Subjects<'a> {
    Dive {
        split: Split<'a, char>,
        current: &'a Subject,
    },

    Browse {
        queue: VecDeque<&'a Subject>,
    },
}

impl<'a> Subjects<'a> {
    pub fn new(path: &'a str, subject: &'a Subject) -> Self {
        Self::Dive {
            split: path.split('/'),
            current: subject,
        }
    }

    pub fn all(root: &'a Subject) -> Self {
        Self::Browse {
            queue: VecDeque::from_iter([root]),
        }
    }
}

impl<'a> Iterator for Subjects<'a> {
    type Item = &'a Subject;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self {
                Subjects::Dive { split, current } => {
                    let path = split.next().unwrap_or_default();

                    if path.trim().is_empty() {
                        let mut queue = VecDeque::new();

                        queue.push_back(*current);

                        *self = Self::Browse { queue };
                        continue;
                    }

                    *current = current.nodes.get(path)?;
                }

                Subjects::Browse { queue } => {
                    let current = queue.pop_front()?;

                    queue.extend(current.nodes.values());

                    return Some(current);
                }
            }
        }
    }
}
