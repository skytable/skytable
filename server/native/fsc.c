/*
 * Created on Fri Aug 07 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

#ifdef __unix__
#define isnix
#elif defined(__WIN32)
#define iswindows
#elif defined(macintosh || Macintosh || (__APPLE__ && __MACH__))
#define ismac
#endif

#ifdef isnix
#include <errno.h>
#include <fcntl.h>

// Lock a file on the record level
int lock_file(int descriptor) {
  if (descriptor < 0) {
    return EBADF; // Bad file descriptor
  }

  struct flock file;
  file.l_type = F_WRLCK;    // Acquire a write-level lock
  file.l_whence = SEEK_SET; // From beginning of file
  file.l_start = 0;         // Lock begins from 0
  file.l_len = 0;           // Lock until EOF

  if (fcntl(descriptor, F_SETLKW, &file) == -1) {
    return errno; // Couldn't get record level lock
  }

  return 0;
}

// Unlock a file on the record level
int unlock_file(int descriptor) {
  struct flock file;

  if (descriptor < 0) {
    return EBADF; // Bad file descriptor
  }

  file.l_type = F_UNLCK;
  file.l_whence = SEEK_SET;
  file.l_len = 0;

  if (fcntl(descriptor, F_SETLKW, &file) == -1) {
    return errno;
  }

  return 0;
}
#endif
