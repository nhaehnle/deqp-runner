use rand::prelude::*;
use rand::distributions::Uniform;

use super::string_pool;
use super::utils::{self, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestRef {
    id: u32,
}
impl Into<AnyRef> for TestRef {
    fn into(self) -> AnyRef {
        AnyRef::Test(self.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnyRef {
    None,
    Test(u32),
    Group(u32),
}
impl AnyRef {
    fn pack(self) -> AnyRefPacked {
        self.into()
    }
}
impl Into<AnyRefPacked> for AnyRef {
    fn into(self) -> AnyRefPacked {
        match self {
        Self::None => AnyRefPacked(u32::MAX),
        Self::Test(idx) => {
            assert!(idx < 1 << 31);
            AnyRefPacked(idx)
        },
        Self::Group(idx) => {
            assert!(idx < 1 << 31 - 1);
            AnyRefPacked((1 << 31) + idx)
        }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AnyRefPacked(u32);
impl AnyRefPacked {
    fn unpack(self) -> AnyRef {
        self.into()
    }
}
impl Into<AnyRef> for AnyRefPacked {
    fn into(self) -> AnyRef {
        if self.0 < 1 << 31 {
            AnyRef::Test(self.0)
        } else if self.0 != u32::MAX {
            AnyRef::Group(self.0 - (1 << 31))
        } else {
            AnyRef::None
        }
    }
}

#[derive(Debug)]
pub struct Suite {
    /// The "path separator" used for nesting tests within groups. Usually ".".
    separator: String,

    tests: Vec<Test>,
    groups: Vec<Group>,

    name_pool: string_pool::Pool,
}

#[derive(Debug)]
struct Test {
    parent: AnyRefPacked,
    name: string_pool::Idx,
}

#[derive(Debug)]
struct Group {
    children: Vec<AnyRefPacked>,
    parent: AnyRefPacked,
    name: string_pool::Idx,
}

impl Suite {
    pub fn new(separator: String) -> Suite {
        Suite {
            separator,
            tests: Vec::new(),
            groups: [Group {
                children: Vec::new(),
                parent: AnyRef::None.into(),
                name: string_pool::Idx::default(),
            }].into(),
            name_pool: string_pool::Pool::new(),
        }
    }

    pub fn tests(&self) -> impl Iterator<Item=TestRef> + '_ {
        (0..self.tests.len() as u32).map(|id| TestRef { id })
    }

    pub fn put(&mut self, mut test: &str) -> Result<TestRef> {
        let mut group_idx = 0;
        while let Some((name, remainder)) = test.split_once(&self.separator) {
            if name.is_empty() {
                return Err("empty group name".into())
            }

            let name_idx = self.name_pool.put(name)?.1;
            let group = &self.groups[group_idx as usize];
            let mut found_child_idx = None;
            for child in &group.children {
                match child.unpack() {
                AnyRef::Test(child_idx) => {
                    if self.tests[child_idx as usize].name == name_idx {
                        return Err("conflict with an existing test".into())
                    }
                },
                AnyRef::Group(child_idx) => {
                    if self.groups[child_idx as usize].name == name_idx {
                        found_child_idx = Some(child_idx);
                        break
                    }
                },
                _ => panic!(),
                }
            }

            group_idx = match found_child_idx {
                Some(child_idx) => child_idx,
                None => {
                    let child_idx = self.groups.len() as u32;
                    if child_idx == u32::MAX {
                        return Err("Suite: too many groups".into())
                    }
                    self.groups[group_idx as usize].children.push(AnyRef::Group(child_idx).into());
                    self.groups.push(Group {
                        children: Vec::new(),
                        parent: AnyRef::Group(group_idx).into(),
                        name: name_idx,
                    });
                    child_idx
                }
                };
            test = remainder;
        }

        let name_idx = self.name_pool.put(test)?.1;
        let group = &self.groups[group_idx as usize];
        for child in &group.children {
            match child.unpack() {
            AnyRef::Test(child_idx) => {
                if self.tests[child_idx as usize].name == name_idx {
                    return Ok(TestRef { id: child_idx });
                }
            },
            AnyRef::Group(child_idx) => {
                if self.groups[child_idx as usize].name == name_idx {
                    return Err("conflict with an existing test".into())
                }
            },
            _ => panic!(),
            }
        }

        let test_idx = self.tests.len() as u32;
        if test_idx == u32::MAX {
            return Err("Suite: too many tests".into());
        }
        self.groups[group_idx as usize].children.push(AnyRef::Test(test_idx).into());
        self.tests.push(Test {
            name: name_idx,
            parent: AnyRef::Group(group_idx).into(),
        });
        Ok(TestRef { id: test_idx })
    }

    fn iter_ancestors(&self, any: AnyRef) -> impl Iterator<Item=u32> + '_ {
        struct Ancestors<'a> {
            suite: &'a Suite,
            group: AnyRef,
        }
        impl<'a> Iterator for Ancestors<'a> {
            type Item = u32;

            fn next(&mut self) -> Option<Self::Item> {
                match self.group {
                AnyRef::Group(parent_idx) => {
                    self.group = self.suite.groups[parent_idx as usize].parent.unpack();
                    Some(parent_idx)
                }
                _ => None,
                }
            }
        }

        Ancestors {
            suite: self,
            group: match any {
                AnyRef::Test(test_idx) => self.tests[test_idx as usize].parent.unpack(),
                AnyRef::Group(group_idx) => self.groups[group_idx as usize].parent.unpack(),
                AnyRef::None => AnyRef::None,
                },
        }
    }

    fn iter_name_indices(&self, any: AnyRef) -> impl Iterator<Item=string_pool::Idx> + '_ {
        let name = match any {
            AnyRef::Test(test_idx) => Some(self.tests[test_idx as usize].name),
            AnyRef::Group(group_idx) => Some(self.groups[group_idx as usize].name),
            AnyRef::None => None,
            };
        name.into_iter()
            .chain(self.iter_ancestors(any).map(|group_idx| self.groups[group_idx as usize].name))
            .filter(|name| !name.is_empty())
    }

    fn iter_name_refs(&self, any: AnyRef) -> impl Iterator<Item=string_pool::Ref> + '_ {
        self.iter_name_indices(any).map(|idx| self.name_pool.ref_by_idx(idx))
    }

    pub fn get_name(&self, test_ref: TestRef) -> String {
        let separator = self.separator.as_bytes();
        let name_bytes = self.iter_name_refs(test_ref.into()).map(|r| r.num_bytes());
        let num_bytes = utils::intersperse(name_bytes, separator.len()).sum();

        let mut bytes: Vec<u8> = std::iter::repeat(0).take(num_bytes).collect();
        let mut end = num_bytes;

        let names = self.iter_name_refs(test_ref.into()).map(|r| self.name_pool.get(r).as_bytes());
        for string in utils::intersperse(names, separator) {
            let begin = end - string.len();
            bytes[begin..end].copy_from_slice(string);
            end = begin;
        }
        assert!(end == 0);

        String::from_utf8(bytes).unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
struct WeightAndCount {
    weight: u64,
    sampled_count: u64,
}
impl WeightAndCount {
    fn zero() -> Self {
        Self {
            weight: u64::MAX,
            sampled_count: 0,
        }
    }

    fn inf() -> Self {
        Self {
            weight: 0,
            sampled_count: u64::MAX,
        }
    }

    fn less_than(self, rhs: Self) -> bool {
        let lhs = self.sampled_count as f64 * rhs.weight as f64;
        let rhs = rhs.sampled_count as f64 * self.weight as f64;
        lhs < rhs
    }
}

#[derive(Debug)]
pub struct Sampler {
    test_weights_cumulative: Vec<u64>,

    test_counts: Vec<u32>,
    names: Vec<WeightAndCount>,
}
impl Sampler {
    /// Create a sampler for the given test suite.
    ///
    /// Tests are weighted according to their "uniqueness" in terms of the names that appear in the test. This gives
    /// relatively less weight to tests in high-dimensional Cartesian products.
    pub fn new(suite: &Suite) -> Result<Sampler> {
        let mut name_frequency: Vec<u32> = std::iter::repeat(0).take(suite.name_pool.string_count()).collect();
        for test_idx in 0..suite.tests.len() {
            for name_idx in suite.iter_name_indices(AnyRef::Test(test_idx as u32)) {
                name_frequency[name_idx.index()] += 1;
            }
        }

        let test_weights: Vec<u64> = (0..suite.tests.len() as u32).map(|test_idx| {
            let mut weight: f64 =
                    suite.iter_name_indices(AnyRef::Test(test_idx))
                        .map(|name_idx| 1.0 / name_frequency[name_idx.index()] as f64).sum();
            if weight > 1.0 {
                weight = 1.0;
            }

            (weight * (1_u64 << 32_u64) as f64) as u64
        }).collect();

        Self::new_with_test_weights(suite, test_weights)
    }

    /// Create a sampler with the given test weights.
    ///
    /// Weights are assigned to test/group names based on the test weights. The final sampled distribution is
    /// consistent with the test weights, but samples are dependent on history to get a smoother exploration of the
    /// test space.
    fn new_with_test_weights(suite: &Suite, mut test_weights: Vec<u64>) -> Result<Sampler> {
        assert!(test_weights.len() == suite.tests.len());

        let mut names: Vec<_> =
                std::iter::repeat(WeightAndCount { weight: 0, sampled_count: 0 })
                    .take(suite.name_pool.string_count()).collect();

        let mut weight_accum = 0;
        for (test_id, test_weight) in test_weights.iter_mut().enumerate() {
            for name in suite.iter_name_indices(AnyRef::Test(test_id as u32)) {
                names[name.index()].weight += *test_weight;
            }

            *test_weight =
                    test_weight.checked_add(weight_accum)
                        .ok_or_else(|| "test suite sampling weights overflow")?;
            weight_accum = *test_weight;
        }

        Ok(Sampler {
            test_weights_cumulative: test_weights,
            test_counts: std::iter::repeat(0).take(suite.tests.len()).collect(),
            names,
        })
    }

    fn sample_core<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> TestRef {
        let total_weight = self.test_weights_cumulative.last().unwrap();
        let r = Uniform::new(0, total_weight).sample(rng);
        let id = self.test_weights_cumulative.partition_point(|&w| w <= r);
        assert!(id < self.test_weights_cumulative.len());
        TestRef { id: id as u32 }
    }

    pub fn sample<R: rand::Rng + ?Sized>(&mut self, suite: &Suite, rng: &mut R) -> TestRef {
        // Use the power of two random choices for a more balanced random exploration of the test suite.
        //
        // First, sample two tests according to the test weights we determined.
        // Then, pick the test which has been sampled least frequently so far.
        // As a tie-breaker, determine which test has a name that was sampled least frequently so far relative to its
        // weight, and pick that test.
        //
        // This causes us to explore the test space randomly, but limits the long-term variance in how often each
        // test and each test name component is picked, which should lead to spreading out tests more effectively
        // for the purpose of finding regressions.
        let sample1 = self.sample_core(rng);
        let sample2 = self.sample_core(rng);

        let sample1_count = self.test_counts[sample1.id as usize];
        let sample2_count = self.test_counts[sample2.id as usize];

        let sample =
            if sample1_count < sample2_count {
                sample1
            } else if sample2_count < sample1_count {
                sample2
            } else {
                let mut least_frequent = WeightAndCount::inf();
                for name in suite.iter_name_indices(sample1.into()) {
                    let weight_and_count = self.names[name.index()];
                    if weight_and_count.less_than(least_frequent) {
                        least_frequent = weight_and_count;
                    }
                }

                let take2 = suite.iter_name_indices(sample2.into()).any(|name| {
                    let weight_and_count = self.names[name.index()];
                    weight_and_count.less_than(least_frequent)
                });

                if take2 { sample2 } else { sample1 }
            };

        self.test_counts[sample.id as usize] += 1;

        for name in suite.iter_name_indices(sample.into()) {
            self.names[name.index()].sampled_count += 1;
        }

        sample
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple() -> Result<()> {
        let mut suite = Suite::new(".".into());

        let test1 = suite.put("group1.test1")?;
        let test2 = suite.put("group1.test2")?;
        let test3 = suite.put("group2.test1")?;
        let test4 = suite.put("test1")?;
        assert_eq!(suite.put("group1.test1")?, test1);
        assert_eq!(suite.get_name(test1), "group1.test1");
        assert_eq!(suite.get_name(test2), "group1.test2");
        assert_eq!(suite.get_name(test3), "group2.test1");
        assert_eq!(suite.get_name(test4), "test1");

        assert!(suite.put("group1.test1.sub").is_err());
        assert!(suite.put("group2").is_err());
        assert!(suite.put("test1.test2").is_err());

        Ok(())
    }
}
