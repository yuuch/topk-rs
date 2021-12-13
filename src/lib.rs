use std::cmp::Ordering;
use std::hash::{Hasher};
use std::collections::hash_map::DefaultHasher;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_type() {
        let bmc: crate::BitmapCounter = vec![crate::CellFSS{error: 1, count:2}];
        assert_eq!(bmc[0].error, 1);
        assert_eq!(bmc[0].count, 2);
    }

    #[test]
    fn test_order_of_element() {
        let e1 = crate::Element{value: 2, estimated_count: 2, associated_error: 3};
        let e2 = crate::Element{value: 2, estimated_count: 2, associated_error: 4};
        assert_eq!(e1.cmp(&e2), Ordering::Greater);
    }
    #[test]
    fn filterd_space_saving_test(){
        let mut fss = FilterdSpaceSaving::new(2, 3);
        assert_eq!(fss.bitmap_counter_size, 2);
        assert_eq!(fss.monitored_list_size_max, 3);
        let element = Element::new(2,3,4);
        println!("{:#?}", element);
        fss.insert_into_monitored_list(element);
        println!("{:#?}", fss.monitored_list[0]);
        assert_eq!(fss.mu, 0);
        let element = Element::new(2,2,4);
        fss.insert_into_monitored_list(element);
        assert_eq!(fss.mu, 0);
    }
    #[test]
    fn topk_test(){
        let stream = vec!['1', '1', '1', '1', '1','2', '2', '2', '3', '4', '4', '4', '4',];
        let mut fss = FilterdSpaceSaving::new(5, 3);
        fss.deal_with_a_stream(stream);
        for ele in fss.monitored_list.iter() {
            println!("{:#?}", ele);
        }
        assert_eq!(fss.monitored_list.len(), 3);
        assert_eq!(fss.monitored_list[0].value, '2');
        assert_eq!(fss.monitored_list[1].value, '4');
        assert_eq!(fss.monitored_list[2].value, '1');
        // assert_eq!(fss.monitored_list[2].value, 2);

    }
}


//Cell for Space Saving
#[derive(Copy, Clone)]
struct CellFSS {
    error: u64, //alpha 
    count: u64, 
}
impl CellFSS {
    fn new() -> Self {
        CellFSS {
            error: 0,
            count: 0,
        }
    }
}
type BitmapCounter = Vec<CellFSS>;

#[derive(Copy, Clone,Debug)]
struct Element<T> {
    value: T, // e in the thesis
    estimated_count: u64, // f in the thesis
    associated_error: u64, // e in the thesis
}

impl<T> Element<T> {
    fn new(value:T, estimated_count: u64, associated_error: u64) -> Self {
        Element {
            value,
            estimated_count,
            associated_error,
        }
    }
}

impl<T> Ord for Element<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.estimated_count.cmp(&other.estimated_count).then(other.associated_error.cmp(&self.associated_error))
    }
}

impl<T> PartialEq for Element<T> {
    fn eq(&self, other: &Self) -> bool {
        &self.estimated_count == &other.estimated_count && &self.associated_error == &other.associated_error
    }
}

impl<T> PartialOrd for Element<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for Element<T> {}

type MonitoredList<T> = Vec<Element<T>>;

struct FilterdSpaceSaving<T> {
    monitored_list: MonitoredList<T>,
    bitmap_counter: BitmapCounter,
    bitmap_counter_size: usize,
    monitored_list_size_max: usize,// the k of topk
    mu: u64, // minimum {f_i}
}

impl<T: std::cmp::PartialEq + std::hash::Hash + std::fmt::Debug + std::fmt::Display> FilterdSpaceSaving<T> {
    fn new(bmc_size: usize, ml_size_max: usize) -> Self {
        FilterdSpaceSaving {
            monitored_list: Vec::new(),
            bitmap_counter: vec![CellFSS::new(); bmc_size],
            bitmap_counter_size: bmc_size,
            monitored_list_size_max: ml_size_max,
            mu: 0,
        }
    }

    fn update_mu(&mut self) {
        // TODO can be optimized.
        self.monitored_list.sort();
        if self.monitored_list.len() != self.monitored_list_size_max {
            self.mu = 0;
        } else {
            self.mu = self.monitored_list[0].estimated_count;
        }
    }

    fn insert_into_monitored_list(&mut self, element: Element<T>) {
            self.increase_bitmap_counter_count(self.hash_fn(&element.value));
            self.monitored_list.push(element);
            self.update_mu()
    }
    
    fn replace_elemnt_in_monitored_list(&mut self, new_element: Element<T>) {
        // deal with the old element
        let old_element = &self.monitored_list[0];
        let k = self.hash_fn(&old_element.value);
        println!("k is {}", k);
        self.decrease_bitmap_counter_count(k);
        self.bitmap_counter[k].error = new_element.estimated_count;

        // replace old with new element
        self.monitored_list[0] = new_element;
        self.increase_bitmap_counter_count(self.hash_fn(&self.monitored_list[0].value));
        self.update_mu();
    }
    
    fn hash_fn(&self, value: &T) -> usize {
        let mut s= DefaultHasher::new();
        value.hash(&mut s);
        let value = s.finish();
        (value as usize % self.bitmap_counter_size).try_into().unwrap()
    }

    fn deal_with_a_stream(&mut self,stream: Vec<T>) {
        for s in stream.into_iter(){
            self.deal_with_new_value(s);
        }
    }

    fn deal_with_new_value(&mut self, value: T) {
        println!("start to deal value {:?}", value);
        let idx = self.hash_fn(&value);
        if self.bitmap_counter[idx].count > 0 {
            let found_idx = self.find_element_in_monitored_list(&value);
            println!("found idx {:?}", found_idx);
            match found_idx {
                Some(i) =>  {
                    println!("found value has exist in the monitor_list");
                    self.increase_monitor_list_count(i);
                    self.increase_bitmap_counter_count(idx);
                    self.update_mu();
                    return
                },
                None => (),
            };
        }

        if self.bitmap_counter[idx].error + 1 >= self.mu {
            let element = Element::new(value, self.bitmap_counter[idx].error, self.bitmap_counter[idx].error + 1);
            if self.monitored_list.len() == self.monitored_list_size_max {
                self.replace_elemnt_in_monitored_list(element);
            } else {
                self.insert_into_monitored_list(element);
            }
        } else {
            self.bitmap_counter[idx].error += 1;
        }
    }

    fn increase_monitor_list_count(&mut self, ml_idx: usize) {
        self.monitored_list[ml_idx].estimated_count += 1;
    }

    fn increase_bitmap_counter_count(&mut self, bmc_idx: usize) {
        println!("count values is {}", self.bitmap_counter[bmc_idx].count);
        self.bitmap_counter[bmc_idx].count += 1;
    }

    fn decrease_bitmap_counter_count(&mut self, bmc_idx: usize) {
        println!("desc bitmap_counter[{}] is {}", bmc_idx, self.bitmap_counter[bmc_idx].count);
        self.bitmap_counter[bmc_idx].count -= 1;
    }
    fn find_element_in_monitored_list(&self, value: &T) -> Option<usize>{
        for (i, ele) in self.monitored_list.iter().enumerate() {
            if ele.value == *value {
                return Some(i)
            }
        }
        return None
    }
}

