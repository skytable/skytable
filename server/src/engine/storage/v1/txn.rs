/*
 * Created on Thu Jul 23 2023
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

/*
  +----------------+------------------------------+-----------------+------------------+--------------------+
  | EVENT ID (16B) | EVENT SOURCE + METADATA (8B) | EVENT MD5 (16B)  | PAYLOAD LEN (8B) | EVENT PAYLOAD (?B) |
  +----------------+------------------------------+-----------------+------------------+--------------------+
  Event ID:
  - The atomically incrementing event ID (for future scale we have 16B; it's like the ZFS situation haha)
  - Event source (1B) + 7B padding (for future metadata)
  - Event MD5; yeah, it's "not as strong as" SHA256 but I've chosen to have it here (since it's sometimes faster and further on,
    we already sum the entire log)
  - Payload len: the size of the pyload
  - Payload: the payload
*/
