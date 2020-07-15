/*
 * Created on Mon Jul 13 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020 Sayan Nandan
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

use super::SELF_VERSION;
use libcore::terrapipe::{
    Dataframe, QueryMetaframe, ResponseBytes, ResponseCodes, DEF_Q_META_BUFSIZE,
};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{self, timeout};

pub struct Connection {
    stream: TcpStream,
}

pub struct Query {
    qmf: QueryMetaframe,
    df: Dataframe,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection { stream }
    }
    pub async fn get_query_packet(&mut self) -> Result<Query, impl ResponseBytes> {
        let mut meta_buffer = String::with_capacity(DEF_Q_META_BUFSIZE);
        let mut bufreader = BufReader::new(&mut self.stream);
        match bufreader.read_line(&mut meta_buffer).await {
            Ok(_) => (),
            Err(_) => {
                return Err(ResponseCodes::InternalServerError);
            }
        }
        let qmf = match QueryMetaframe::from_buffer(&SELF_VERSION, &meta_buffer) {
            Ok(qmf) => qmf,
            Err(e) => return Err(e),
        };
        let mut data_buffer = vec![0; qmf.get_content_size()];
        match timeout(
            time::Duration::from_millis(400),
            bufreader.read(&mut data_buffer),
        )
        .await
        {
            Ok(_) => (),
            Err(_) => return Err(ResponseCodes::InternalServerError),
        }
        let df = match Dataframe::from_buffer(qmf.get_content_size(), data_buffer) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        Ok(Query { qmf, df })
    }
    pub async fn write_response_packet(&mut self, resp: Vec<u8>) -> io::Result<()> {
        self.stream.write_all(&resp).await
    }
}
