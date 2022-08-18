/*
 * Created on Fri Jul 30 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

mod sdel_concurrency_tests {
    use {
        super::super::sdel,
        crate::{corestore::SharedSlice, kvengine::KVEStandard},
        std::{sync::Arc, thread},
    };

    #[test]
    fn test_snapshot_okay() {
        let kve = KVEStandard::init(true, true);
        kve.upsert(SharedSlice::from("k1"), SharedSlice::from("v1"))
            .unwrap();
        kve.upsert(SharedSlice::from("k2"), SharedSlice::from("v2"))
            .unwrap();
        let encoder = kve.get_key_encoder();
        let it = bi!("k1", "k2");
        let ret = sdel::snapshot_and_del(&kve, encoder, it.as_ref().iter().as_ref().iter());
        assert!(ret.is_ok());
    }
    #[test]
    fn test_sdel_snapshot_fail_with_t2() {
        let kve = Arc::new(KVEStandard::init(true, true));
        let kve1 = kve.clone();
        let encoder = kve.get_key_encoder();
        {
            kve.upsert(SharedSlice::from("k1"), SharedSlice::from("v1"))
                .unwrap();
            kve.upsert(SharedSlice::from("k2"), SharedSlice::from("v2"))
                .unwrap();
        }
        let it = bi!("k1", "k2");
        // sdel will wait 10s for us
        let t1handle = thread::spawn(move || {
            sdel::snapshot_and_del(&kve1, encoder, it.as_ref().iter().as_ref().iter())
        });
        // we have 10s: we sleep 5 to let the snapshot complete (thread spawning takes time)
        do_sleep!(5 s);
        assert!(kve
            .update(SharedSlice::from("k1"), SharedSlice::from("updated-v1"))
            .unwrap());
        // let us join t1
        let ret = t1handle.join().unwrap();
        assert!(ret.is_ok());
        // although we told sdel to delete it, it.as_ref().iter() shouldn't because we externally
        // updated the value
        assert!(kve.exists(&SharedSlice::from("k1")).unwrap());
    }
}

mod sset_concurrency_tests {
    use {
        super::super::sset,
        crate::{corestore::SharedSlice, kvengine::KVEStandard},
        std::{sync::Arc, thread},
    };

    #[test]
    fn test_snapshot_okay() {
        let kve = KVEStandard::init(true, true);
        let encoder = kve.get_double_encoder();
        let it = bi!("k1", "v1", "k2", "v2");
        let ret = sset::snapshot_and_insert(&kve, encoder, it.as_ref().iter());
        assert!(ret.is_ok());
    }
    #[test]
    fn test_sset_snapshot_fail_with_t2() {
        let kve = Arc::new(KVEStandard::init(true, true));
        let kve1 = kve.clone();
        let encoder = kve.get_double_encoder();
        let it = bi!("k1", "v1", "k2", "v2");
        // sset will wait 10s for us
        let t1handle =
            thread::spawn(move || sset::snapshot_and_insert(&kve1, encoder, it.as_ref().iter()));
        // we have 10s: we sleep 5 to let the snapshot complete (thread spawning takes time)
        do_sleep!(5 s);
        // update the value externally
        assert!(kve
            .set(SharedSlice::from("k1"), SharedSlice::from("updated-v1"))
            .unwrap());
        // let us join t1
        let ret = t1handle.join().unwrap();
        // but set won't fail because someone set it before it did; this is totally
        // acceptable because we only wanted to set it if it matches the status when
        // we created a snapshot
        assert!(ret.is_ok());
        // although we told sset to set a key, but it shouldn't because we updated it
        assert_eq!(
            kve.get(&SharedSlice::from("k1")).unwrap().unwrap().clone(),
            SharedSlice::from("updated-v1")
        );
    }
}

mod supdate_concurrency_tests {
    use {
        super::super::supdate,
        crate::{corestore::SharedSlice, kvengine::KVEStandard},
        std::{sync::Arc, thread},
    };

    #[test]
    fn test_snapshot_okay() {
        let kve = KVEStandard::init(true, true);
        kve.upsert(SharedSlice::from("k1"), SharedSlice::from("v1"))
            .unwrap();
        kve.upsert(SharedSlice::from("k2"), SharedSlice::from("v2"))
            .unwrap();
        let encoder = kve.get_double_encoder();
        let it = bi!("k1", "v1", "k2", "v2");
        let ret = supdate::snapshot_and_update(&kve, encoder, it.as_ref().iter());
        assert!(ret.is_ok());
    }
    #[test]
    fn test_supdate_snapshot_fail_with_t2() {
        let kve = Arc::new(KVEStandard::init(true, true));
        kve.upsert(SharedSlice::from("k1"), SharedSlice::from("v1"))
            .unwrap();
        kve.upsert(SharedSlice::from("k2"), SharedSlice::from("v2"))
            .unwrap();
        let kve1 = kve.clone();
        let encoder = kve.get_double_encoder();
        let it = bi!("k1", "v1", "k2", "v2");
        // supdate will wait 10s for us
        let t1handle =
            thread::spawn(move || supdate::snapshot_and_update(&kve1, encoder, it.as_ref().iter()));
        // we have 10s: we sleep 5 to let the snapshot complete (thread spawning takes time)
        do_sleep!(5 s);
        // lets update the value externally
        assert!(kve
            .update(SharedSlice::from("k1"), SharedSlice::from("updated-v1"))
            .unwrap());
        // let us join t1
        let ret = t1handle.join().unwrap();
        assert!(ret.is_ok());
        // although we told supdate to update the key, it.as_ref().iter() shouldn't because we updated it
        // externally; hence our `updated-v1` value should persist
        assert_eq!(
            kve.get(&SharedSlice::from("k1")).unwrap().unwrap().clone(),
            SharedSlice::from("updated-v1")
        );
    }
}
