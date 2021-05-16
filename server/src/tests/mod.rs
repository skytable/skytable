/*
 * Created on Tue Aug 25 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! This module contains automated tests for queries

mod kvengine;

mod bgsave {
    use crate::config::BGSave;
    use crate::coredb::{htable::HTable, CoreDB, Data};
    use crate::dbnet::Terminator;
    use crate::diskstore;
    use crate::services;
    use services::bgsave::BGSAVE_DIRECTORY_TESTING_LOC;
    use std::fs;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use tokio::time::{self, Duration};
    #[tokio::test]
    async fn test_bgsave() {
        // pre-initialize our maps for comparison
        let mut map_should_be_with_one = HTable::new();
        map_should_be_with_one.insert(
            String::from("sayan"),
            Data::from_string("is testing bgsave".to_owned()),
        );
        #[allow(non_snake_case)]
        let DUR_WITH_EPSILON: Duration = Duration::from_millis(1500) + Duration::from_secs(10);
        let (signal, _) = broadcast::channel(1);
        let datahandle = CoreDB::new_empty(Arc::new(None));
        let bgsave_configuration = BGSave::Enabled(10);
        let handle = tokio::spawn(services::bgsave::bgsave_scheduler(
            datahandle.clone(),
            bgsave_configuration,
            Terminator::new(signal.subscribe()),
        ));
        // sleep for 10 seconds with epsilon 1.5s
        time::sleep(DUR_WITH_EPSILON).await;
        // we should get an empty map
        let saved = diskstore::test_deserialize(fs::read(BGSAVE_DIRECTORY_TESTING_LOC).unwrap()).unwrap();
        assert!(saved.len() == 0);
        // now let's quickly write some data
        {
            datahandle.acquire_write().unwrap().get_mut_ref().insert(
                String::from("sayan"),
                Data::from_string("is testing bgsave".to_owned()),
            );
        }
        // sleep for 10 seconds with epsilon 1.5s
        time::sleep(DUR_WITH_EPSILON).await;
        // we should get a map with the one key
        let saved = diskstore::test_deserialize(fs::read(BGSAVE_DIRECTORY_TESTING_LOC).unwrap()).unwrap();
        assert_eq!(saved, map_should_be_with_one);
        // now let's remove all the data
        {
            datahandle.acquire_write().unwrap().get_mut_ref().clear();
        }
        // sleep for 10 seconds with epsilon 1.5s
        time::sleep(DUR_WITH_EPSILON).await;
        let saved = diskstore::test_deserialize(fs::read(BGSAVE_DIRECTORY_TESTING_LOC).unwrap()).unwrap();
        assert!(saved.len() == 0);
        // drop the signal; all waiting tasks can now terminate
        drop(signal);
        handle.await.unwrap();
        // check the file again after unlocking
        let saved = diskstore::test_deserialize(fs::read(BGSAVE_DIRECTORY_TESTING_LOC).unwrap()).unwrap();
        assert!(saved.len() == 0);
        fs::remove_file(BGSAVE_DIRECTORY_TESTING_LOC).unwrap();
    }
}
