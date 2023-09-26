/*
 * Created on Sat Jul 29 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

type VirtualFS = super::memfs::VirtualFS;

mod batch;
mod rw;
mod tx;

mod sysdb {
    use {
        super::{
            super::sysdb::{self, SystemStoreInitState},
            VirtualFS as VFS,
        },
        crate::engine::config::{AuthDriver, ConfigAuth},
    };
    #[test]
    fn simple_open_close() {
        {
            let syscfg_new = sysdb::open_or_reinit_system_database::<VFS>(
                None,
                "sysdb_test_1.db",
                "sysdb_test_1.cow.db",
            )
            .unwrap();
            assert_eq!(syscfg_new.state, SystemStoreInitState::Created);
            assert!(syscfg_new.store.auth_data().is_none());
            assert_eq!(syscfg_new.store.host_data().settings_version(), 0);
            assert_eq!(syscfg_new.store.host_data().startup_counter(), 0);
        }
        let syscfg_restore = sysdb::open_or_reinit_system_database::<VFS>(
            None,
            "sysdb_test_1.db",
            "sysdb_test_1.cow.db",
        )
        .unwrap();
        assert_eq!(syscfg_restore.state, SystemStoreInitState::Unchanged);
        assert!(syscfg_restore.store.auth_data().is_none());
        assert_eq!(syscfg_restore.store.host_data().settings_version(), 0);
        assert_eq!(syscfg_restore.store.host_data().startup_counter(), 1);
    }
    #[test]
    fn with_auth_nochange() {
        let auth = ConfigAuth::new(AuthDriver::Pwd, "password12345678".to_string());
        {
            let syscfg_new = sysdb::open_or_reinit_system_database::<VFS>(
                Some(auth.clone()),
                "sysdb_test_2.db",
                "sysdb_test_2.cow.db",
            )
            .unwrap();
            assert_eq!(syscfg_new.state, SystemStoreInitState::Created);
            assert!(syscfg_new
                .store
                .auth_data()
                .as_ref()
                .unwrap()
                .read()
                .verify_user("root", "password12345678")
                .is_ok());
            assert_eq!(syscfg_new.store.host_data().startup_counter(), 0);
            assert_eq!(syscfg_new.store.host_data().settings_version(), 0);
        }
        // now reboot
        let syscfg_new = sysdb::open_or_reinit_system_database::<VFS>(
            Some(auth),
            "sysdb_test_2.db",
            "sysdb_test_2.cow.db",
        )
        .unwrap();
        assert_eq!(syscfg_new.state, SystemStoreInitState::Unchanged);
        assert!(syscfg_new
            .store
            .auth_data()
            .as_ref()
            .unwrap()
            .read()
            .verify_user("root", "password12345678")
            .is_ok());
        assert_eq!(syscfg_new.store.host_data().startup_counter(), 1);
        assert_eq!(syscfg_new.store.host_data().settings_version(), 0);
    }
    #[test]
    fn disable_auth() {
        {
            let auth = ConfigAuth::new(AuthDriver::Pwd, "password12345678".to_string());
            let syscfg_new = sysdb::open_or_reinit_system_database::<VFS>(
                Some(auth),
                "sysdb_test_3.db",
                "sysdb_test_3.cow.db",
            )
            .unwrap();
            assert_eq!(syscfg_new.state, SystemStoreInitState::Created);
            assert!(syscfg_new
                .store
                .auth_data()
                .as_ref()
                .unwrap()
                .read()
                .verify_user("root", "password12345678")
                .is_ok());
            assert_eq!(syscfg_new.store.host_data().startup_counter(), 0);
            assert_eq!(syscfg_new.store.host_data().settings_version(), 0);
        }
        // reboot
        let sysdb_cfg = sysdb::open_or_reinit_system_database::<VFS>(
            None,
            "sysdb_test_3.db",
            "sysdb_test_3.cow.db",
        )
        .unwrap();
        assert_eq!(sysdb_cfg.state, SystemStoreInitState::UpdatedAuthDisabled);
        assert!(sysdb_cfg.store.auth_data().is_none());
        assert_eq!(sysdb_cfg.store.host_data().startup_counter(), 1);
        assert_eq!(sysdb_cfg.store.host_data().settings_version(), 1);
    }
    #[test]
    fn enable_auth() {
        {
            let sysdb_cfg = sysdb::open_or_reinit_system_database::<VFS>(
                None,
                "sysdb_test_4.db",
                "sysdb_test_4.cow.db",
            )
            .unwrap();
            assert_eq!(sysdb_cfg.state, SystemStoreInitState::Created);
            assert!(sysdb_cfg.store.auth_data().is_none());
            assert_eq!(sysdb_cfg.store.host_data().startup_counter(), 0);
            assert_eq!(sysdb_cfg.store.host_data().settings_version(), 0);
        }
        // reboot
        let auth = ConfigAuth::new(AuthDriver::Pwd, "password12345678".to_string());
        let syscfg_new = sysdb::open_or_reinit_system_database::<VFS>(
            Some(auth),
            "sysdb_test_4.db",
            "sysdb_test_4.cow.db",
        )
        .unwrap();
        assert_eq!(syscfg_new.state, SystemStoreInitState::UpdatedAuthEnabled);
        assert!(syscfg_new
            .store
            .auth_data()
            .as_ref()
            .unwrap()
            .read()
            .verify_user("root", "password12345678")
            .is_ok());
        assert_eq!(syscfg_new.store.host_data().startup_counter(), 1);
        assert_eq!(syscfg_new.store.host_data().settings_version(), 1);
    }
}
