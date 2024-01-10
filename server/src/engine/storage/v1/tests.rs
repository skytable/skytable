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

type VirtualFS = crate::engine::storage::common::interface::fs_test::VirtualFS;

mod batch;
mod rw;
mod tx;

mod sysdb {
    use {
        super::{super::sysdb::SystemStoreInitState, VirtualFS as VFS},
        crate::engine::{
            config::{AuthDriver, ConfigAuth, ConfigMode},
            fractal::sys_store::SystemStore,
        },
    };
    fn open_sysdb(
        auth_config: ConfigAuth,
        sysdb_path: &str,
        sysdb_cow_path: &str,
    ) -> (SystemStore<VFS>, SystemStoreInitState) {
        SystemStore::<VFS>::open_with_name(sysdb_path, sysdb_cow_path, auth_config, ConfigMode::Dev)
            .unwrap()
    }
    #[test]
    fn open_close() {
        let open = |auth_config| {
            open_sysdb(
                auth_config,
                "open_close_test.sys.db",
                "open_close_test.sys.cow.db",
            )
        };
        let auth_config = ConfigAuth::new(AuthDriver::Pwd, "password12345678".into());
        {
            let (config, state) = open(auth_config.clone());
            assert_eq!(state, SystemStoreInitState::Created);
            assert!(config
                .system_store()
                .auth_data()
                .read()
                .verify_user("root", "password12345678")
                .is_ok());
            assert_eq!(config.system_store().host_data().settings_version(), 0);
            assert_eq!(config.system_store().host_data().startup_counter(), 0);
        }
        // reboot
        let (config, state) = open(auth_config);
        assert_eq!(state, SystemStoreInitState::Unchanged);
        assert!(config
            .system_store()
            .auth_data()
            .read()
            .verify_user("root", "password12345678")
            .is_ok());
        assert_eq!(config.system_store().host_data().settings_version(), 0);
        assert_eq!(config.system_store().host_data().startup_counter(), 1);
    }
    #[test]
    fn open_change_root_password() {
        let open = |auth_config| {
            open_sysdb(
                auth_config,
                "open_change_root_password.sys.db",
                "open_change_root_password.sys.cow.db",
            )
        };
        {
            let (config, state) = open(ConfigAuth::new(AuthDriver::Pwd, "password12345678".into()));
            assert_eq!(state, SystemStoreInitState::Created);
            assert!(config
                .system_store()
                .auth_data()
                .read()
                .verify_user("root", "password12345678")
                .is_ok());
            assert_eq!(config.system_store().host_data().settings_version(), 0);
            assert_eq!(config.system_store().host_data().startup_counter(), 0);
        }
        let (config, state) = open(ConfigAuth::new(AuthDriver::Pwd, "password23456789".into()));
        assert_eq!(state, SystemStoreInitState::UpdatedRoot);
        assert!(config
            .system_store()
            .auth_data()
            .read()
            .verify_user("root", "password23456789")
            .is_ok());
        assert_eq!(config.system_store().host_data().settings_version(), 1);
        assert_eq!(config.system_store().host_data().startup_counter(), 1);
    }
}
