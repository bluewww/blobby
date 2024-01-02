# Robert Balas <balasr@iis.ee.ethz.ch>
# SPDX-License-Identifier: MIT

import argparse
import zlib
from collections import namedtuple
from pathlib import Path
from pprint import pprint

# commit is commit {content size}{null byte}{content}
# blob is blob {content size}{null byte}{content}
# tree is tree {content size}{null byte}[list of entries]
#    entry is {mode} {filename}{null byte}{sha1 of tree or blob in binary}


def consume_bytes(data, size):
    return data[:size], data[size:]


def consume_bytes_to_sep(data, sep):
    """Consume all bytes between data[0] and next sep"""
    for i, b in enumerate(data):
        if b == int.from_bytes(sep):
            return data[:i], data[i+1:]

    raise Exception('Unable to find separator', sep)


def read_content_size(data):
    return consume_bytes_to_sep(data, b'\x00')


def read_entry(data):
    """Read a tree entry

    entry is {mode} {filename}{null byte}{sha1 of tree or blob in binary}
    """
    mode, data = consume_bytes_to_sep(data, b' ')
    filename, data = consume_bytes_to_sep(data, b'\x00')
    sha1, data = consume_bytes(data, 20)
    return mode, filename, sha1, data


git_object = namedtuple('git_object', 'gtype size content')


def read_git_object(path):
    """Read a git object which can either be a binary large object (blob) or a
    tree
    """
    with open(path, 'rb') as f:
        data = zlib.decompress(f.read())

        if data.startswith(b'tree'):
            _, data = consume_bytes_to_sep(data, b' ')
            size, data = read_content_size(data)
            entries = []
            while len(data) > 0:
                mode, filename, sha1, data = read_entry(data)
                entries.append((mode, filename, sha1))
            return git_object('tree', size, entries)

        elif data.startswith(b'commit'):
            _, data = consume_bytes_to_sep(data, b' ')
            size, data = read_content_size(data)
            content = data
            return git_object('commit', size, content.decode())
        elif data.startswith(b'blob'):
            _, data = consume_bytes_to_sep(data, b' ')
            size, data = read_content_size(data)
            content = data
            return git_object('blob', size, content.decode())
        else:
            raise Exception('unknown git object')


obj_types = {
    0x01: 'OBJ_COMMIT',
    0x02: 'OBJ_TREE',
    0x03: 'OBJ_BLOB',
    0x04: 'OBJ_TAG',
    0x06: 'OBJ_OFS_DELTA',
    0x07: 'OBJ_REF_DELTA'
}

obj_types_inv = {}
for k, v in obj_types.items():
    obj_types_inv[v] = k


def read_packed_index(data):
    pass


def read_packed_object_entry(data):
    """Valid git packed object types are
    OBJ_COMMIT (1)
    OBJ_TREE (2)
    OBJ_BLOB (3)
    OBJ_TAG (4)
    OBJ_OFS_DELTA (6)
    OBJ_REF_DELTA (7)
    """
    size_type_raw, data = consume_bytes(data, 1)
    obj_type = (size_type_raw[0] & b'\x70'[0]) >> 4
    obj_size = size_type_raw[0] & b'\x0f'[0]

    if size_type_raw[0] & b'\x80'[0] != 0:
        size_type_raw, data = consume_bytes(data, 1)
        obj_size = obj_size | ((size_type_raw[0] & b'\x7f'[0]) << 4)

    k = 0
    while size_type_raw[0] & b'\x80'[0] != 0:
        size_type_raw, data = consume_bytes(data, 1)
        obj_size = obj_size | ((size_type_raw[0] & b'\x7f'[0]) << (11 + 7 * k))
        k = k + 1

    obj = None
    dco = zlib.decompressobj(wbits=zlib.MAX_WBITS | 32)

    if obj_type == obj_types_inv['OBJ_OFS_DELTA']:
        k = 0
        offset = 0
        offset_raw = b'\x80'
        while offset_raw[0] & b'\x80'[0] != 0:
            offset_raw, data = consume_bytes(data, 1)
            offset = offset | ((offset_raw[0] & b'\x7f'[0])
                               << (7 * k))
            k = k + 1
        offset = -offset
        obj = dco.decompress(data)
        data = dco.unused_data

    elif obj_type == obj_types_inv['OBJ_REF_DELTA']:
        obj, data = consume_bytes(data, 20)
        obj += dco.decompress(data)
        data = dco.unused_data

    elif (obj_type == obj_types_inv['OBJ_COMMIT'] or
          obj_type == obj_types_inv['OBJ_TREE'] or
          obj_type == obj_types_inv['OBJ_BLOB'] or
          obj_type == obj_types_inv['OBJ_TAG']):
        obj = dco.decompress(data)

        if len(obj) != obj_size:
            raise Exception("""obj_size does not matched size of decompressed
            packed object""")

        data = dco.unused_data
    else:
        raise Exception('unknown packed object type', obj_type)

    return obj_type, obj_size, obj, data


git_packed = namedtuple('git_packed',
                        'version num_objs objs')
git_packed_object = namedtuple('git_packed_object',
                               'ptype size real_size content')


def read_git_packed(path):
    """Read a git pack file"""
    with open(path, 'rb') as f:
        data = f.read()
        if not data.startswith(b'PACK'):
            raise Exception("""this is not a git pack file or the header is
damaged""")

        _, data = consume_bytes(data, 4)

        version, data = consume_bytes(data, 4)
        if int.from_bytes(version, 'big') != 2:
            raise Exception('unsupported git packed format version')

        num_objs, data = consume_bytes(data, 4)

        packed_objs = []
        while data:
            dsize = len(data)
            obj_type, obj_size, obj, data = read_packed_object_entry(data)
            packed_objs.append(git_packed_object(
                obj_type, obj_size, dsize - len(data), obj))

        return git_packed(version, num_objs, packed_objs)


def read_git_repository(path):
    """Reads a git repository and dumps meta information about all objects
    """
    gitdir = path / '.git'

    if not gitdir.exists():
        raise Exception(path, " does not contain a .git directory")

    objdir = gitdir / 'objects'

    # Handle all the loose objects
    for objtopdir in (dd for dd in objdir.iterdir()
                      if dd.is_dir() if len(dd.stem) == 2):
        for obj in objtopdir.iterdir():
            obj = read_git_object(obj)
            pprint(obj.gtype)
            pprint(obj.size)
            pprint(obj.content)
            print()

    # Handle all the packed objects
    packdir = objdir / 'pack'

    for packed in (p for p in packdir.iterdir() if p.name.endswith('.pack')):
        objs = read_git_packed(packed)
        pprint(objs.version)
        pprint(objs.num_objs)
        for obj in objs.objs:
            pprint(obj_types[obj.ptype])
            pprint(obj.size)
            pprint(obj.real_size)
            pprint(obj.content)
            print()


parser = argparse.ArgumentParser(prog='blobby',
                                 description="""Read and dump git objects""")

parser.version = '0.1'
parser.add_argument('repo', type=str, help='path to git repository')

args = parser.parse_args()

if __name__ == '__main__':
    read_git_repository(Path(args.repo))
