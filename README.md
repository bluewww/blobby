# Blobby
Reads and dumps git objects (loose and packed) from a git repository. This
script demonstrates how to parse this data.

Most of the information can be gleamed from the [git documentation](https://git-scm.com/docs/pack-format) 
itself though some parts are lacking such as the description of how the offset
in `OFS_DELTA`s is encoded.

## Usage

	./blobby.py path-to-your-repository
