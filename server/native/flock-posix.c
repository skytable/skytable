/*
 * Created on Fri Aug 07 2020
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */

#include <errno.h>
#include <sys/file.h>

/* Acquire an exclusive lock for a file with the given descriptor */
int lock_exclusive(int descriptor) {
  if (descriptor < 0) {
    return EBADF;
  }
  if (flock(descriptor, LOCK_EX) == -1) {
    return errno;
  }
  return 0;
}

int try_lock_exclusive(int descriptor) {
  if (descriptor < 0) {
    return EBADF;
  }
  if (flock(descriptor, LOCK_EX | LOCK_NB) == -1) {
    return errno;
  }
  return 0;
}

/* Unlock a file with the given descriptor */
int unlock(int descriptor) {
  if (descriptor < 0) {
    return EBADF;
  }
  if (flock(descriptor, LOCK_UN) == -1) {
    return errno;
  }
  return 0;
}