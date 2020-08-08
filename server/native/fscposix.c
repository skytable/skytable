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

#include <errno.h>
#include <fcntl.h>

/* Lock a file */
int lock_file(int descriptor) {
  if (descriptor < 0) {
    return EBADF;
  }

  struct flock fl;

  /* Lock the whole file - not just a part of it! */
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;

  if (fcntl(descriptor, F_SETLKW, &fl) == -1) {
    return errno;
  }

  return 0;
}

/* Unlock a file */
int unlock_file(int descriptor) {
  struct flock fl;

  if (descriptor < 0) {
    return EBADF;
  }

  /* Unlock the whole file - not just a part of it! */
  fl.l_type = F_UNLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;

  if (fcntl(descriptor, F_SETLKW, &fl) == -1) {
    return errno;
  }

  return 0;
}
