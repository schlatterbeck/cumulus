#!/usr/bin/perl -w
#
# Proof-of-concept/reference decoder for LBS-format backup snapshots.
#
# This decoder aims to decompress an LBS snapshot.  It is not meant to be
# particularly efficient, but should be a small and portable tool for doing so
# (important for recovering from data loss).  It is also meant to serve as a
# check on the snapshot tool and data format itself, and serve as documentation
# for the format.
#
# This decoder does not understand TAR archives; it assumes that all segments
# in the snapshot have already been decompressed, and that objects are
# available simply as files in the filesystem.  This simplifies the design.
#
# Limitations: Since this code is probably using 32-bit arithmetic, files
# larger than 2-4 GB may not be properly handled.
#
# Copyright (C) 2007  Michael Vrable

use strict;
use Digest::SHA1;
use File::Basename;

my $OBJECT_DIR;                 # Where are the unpacked objects available?
my $DEST_DIR = ".";             # Where should restored files should be placed?
my $RECURSION_LIMIT = 3;        # Bound on recursive object references

my $VERBOSE = 0;                # Set to 1 to enable debugging messages

############################ CHECKSUM VERIFICATION ############################
# A very simple layer for verifying checksums.  Checksums may be used on object
# references directly, and can also be used to verify entire reconstructed
# files.
#
# A checksum to verify is given in the form "algorithm=hexdigest".  Given such
# a string, we can construct a "verifier" object.  Bytes can be incrementally
# added to the verifier, and at the end a test can be made to see if the
# checksum matches.  The caller need not know what algorithm is used.  However,
# at the moment we only support SHA-1 for computing digest (algorith name
# "sha1").
sub verifier_create {
    my $checksum = shift;

    if ($checksum !~ m/^(\w+)=([0-9a-f]+)$/) {
        die "Malformed checksum: $checksum";
    }
    my ($algorithm, $hash) = ($1, $2);
    if ($algorithm ne 'sha1') {
        die "Unsupported checksum algorithm: $algorithm";
    }

    my %verifier = (
        ALGORITHM => $algorithm,
        HASH => $hash,
        DIGESTER => new Digest::SHA1
    );

    return \%verifier;
}

sub verifier_add_bytes {
    my $verifier = shift;
    my $digester = $verifier->{DIGESTER};
    my $data = shift;

    $digester->add($data);
}

sub verifier_check {
    my $verifier = shift;
    my $digester = $verifier->{DIGESTER};

    my $newhash = $digester->hexdigest();
    if ($VERBOSE && $verifier->{HASH} ne $newhash) {
        print STDERR "Verification failure: ",
            $newhash, " != ", $verifier->{HASH}, "\n";
    }
    return ($verifier->{HASH} eq $newhash);
}

################################ OBJECT ACCESS ################################
# The base of the decompressor is the object reference layer.  See ref.h for a
# description of the format for object references.  These functions will parse
# an object reference, locate the object data from the filesystem, perform any
# necessary integrity checks (if a checksum is included), and return the object
# data.
sub load_ref {
    my $ref_str = shift;

    # Check for special objects before attempting general parsing.
    if ($ref_str =~ m/^zero\[((\d+)\+)?(\d+)\]$/) {
        return "\0" x ($3 + 0);
    }

    # Try to parse the object reference string into constituent pieces.  The
    # format is segment/object(checksum)[range].  Both the checksum and range
    # are optional.
    if ($ref_str !~ m/^([-0-9a-f]+)\/([0-9a-f]+)(\(\S+\))?(\[\S+\])?$/) {
        die "Malformed object reference: $ref_str";
    }

    my ($segment, $object, $checksum, $range) = ($1, $2, $3, $4);

    # Next, use the segment/object components to locate and read the object
    # contents from disk.
    open OBJECT, "<", "$OBJECT_DIR/$segment/$object"
        or die "Unable to open object $OBJECT_DIR/$segment/$object: $!";
    my $contents = join '', <OBJECT>;
    close OBJECT;

    # If a checksum was specified in the object reference, verify the object
    # integrity by computing a checksum of the read data and comparing.
    if ($checksum) {
        $checksum =~ m/^\((\S+)\)$/;
        my $verifier = verifier_create($1);
        verifier_add_bytes($verifier, $contents);
        if (!verifier_check($verifier)) {
            die "Integrity check for object $ref_str failed";
        }
    }

    # If a range was specified, then only a subset of the bytes of the object
    # are desired.  Extract just the desired bytes.
    if ($range) {
        if ($range !~ m/^\[((\d+)\+)?(\d+)\]$/) {
            die "Malformed object range: $range";
        }

        my $object_size = length $contents;
        my ($start, $length);
        if ($1 ne "") {
            ($start, $length) = ($2 + 0, $3 + 0);
        } else {
            ($start, $length) = (0, $3 + 0);
        }
        if ($start >= $object_size || $start + $length > $object_size) {
            die "Object range $range falls outside object bounds "
                . "(actual size $object_size)";
        }

        $contents = substr $contents, $start, $length;
    }

    return $contents;
}

############################### FILE PROCESSING ###############################
# Process the metadata for a single file.  process_file is the main entry
# point; it should be given a list of file metadata key/value pairs.
# iterate_objects is a helper function used to iterate over the set of object
# references that contain the file data for a regular file.

sub parse_int {
    my $str = shift;
    if ($str =~ /^0/) {
        return oct($str);
    } else {
        return $str + 0;
    }
}

sub uri_decode {
    my $str = shift;
    $str =~ s/%([0-9a-f]{2})/chr(hex($1))/ge;
    return $str;
}

sub iterate_objects {
    my $callback = shift;       # Function to be called for each reference
    my $arg = shift;            # Argument passed to callback
    my $text = shift;           # Whitespace-separate list of object references

    # Simple limit to guard against cycles in the object references
    my $recursion_level = shift || 0;
    if ($recursion_level >= $RECURSION_LIMIT) {
        die "Recursion limit reached";
    }

    # Split the provided text at whitespace boundaries to produce the list of
    # object references.  If any of these start with "@", then we have an
    # indirect reference, and must look up that object and call iterate_objects
    # with the contents.
    my $obj;
    foreach $obj (split /\s+/, $text) {
        next if $obj eq "";
        if ($obj =~ /^@(\S+)$/) {
            my $indirect = load_ref($1);
            iterate_objects($callback, $arg, $indirect, $recursion_level + 1);
        } else {
            &$callback($arg, $obj);
        }
    }
}

sub obj_callback {
    my $state = shift;
    my $obj = shift;
    my $data = load_ref($obj);
    print FILE $data
        or die "Error writing file data: $!";
    verifier_add_bytes($state->{VERIFIER}, $data);
    $state->{BYTES} += length($data);
}

# Extract the contents of a regular file by concatenating all the objects that
# comprise it.
sub unpack_file {
    my $name = shift;
    my %info = @_;
    my %state = ();

    if (!defined $info{data}) {
        die "File contents not specified for $name";
    }
    if (!defined $info{checksum} || !defined $info{size}) {
        die "File $name is missing checksum or size";
    }

    $info{size} = parse_int($info{size});

    # Open the file to be recreated.  The data will be written out by the call
    # to iterate_objects.
    open FILE, ">", "$DEST_DIR/$name"
        or die "Cannot write file $name: $!";

    # Set up state so that we can incrementally compute the checksum and length
    # of the reconstructed data.  Then iterate over all objects in the file.
    $state{VERIFIER} = verifier_create($info{checksum});
    $state{BYTES} = 0;
    iterate_objects(\&obj_callback, \%state, $info{data});

    close FILE;

    # Verify that the reconstructed object matches the size/checksum we were
    # given.
    if (!verifier_check($state{VERIFIER}) || $state{BYTES} != $info{size}) {
        die "File reconstruction failed for $name: size or checksum differs";
    }
}

sub process_file {
    my %info = @_;

    if (!defined($info{name})) {
        die "Filename not specified in metadata block";
    }

    my $type = $info{type};

    my $filename = uri_decode($info{name});
    print "$filename\n" if $VERBOSE;

    # Restore the specified file.  How to do so depends upon the file type, so
    # dispatch based on that.
    my $dest = "$DEST_DIR/$filename";
    if ($type eq '-' || $type eq 'f') {
        # Regular file
        unpack_file($filename, %info);
    } elsif ($type eq 'd') {
        # Directory
        if ($filename ne '.') {
            mkdir $dest or die "Cannot create directory $filename: $!";
        }
    } elsif ($type eq 'l') {
        # Symlink
        my $target = $info{target} || $info{contents};
        if (!defined($target)) {
            die "Symlink $filename has no value specified";
        }
        $target = uri_decode($target);
        symlink $target, $dest
            or die "Cannot create symlink $filename: $!";

        # TODO: We can't properly restore all metadata for symbolic links
        # (attempts to do so below will change metadata for the pointed-to
        # file).  This should be later fixed, but for now we simply return
        # before getting to the restore metadata step below.
        return;
    } elsif ($type eq 'p' || $type eq 's' || $type eq 'c' || $type eq 'b') {
        # Pipe, socket, character device, block device.
        # TODO: Handle these cases.
        print STDERR "Ignoring special file $filename of type $type\n";
        return;
    } else {
        die "Unknown file type '$type' for file $filename";
    }

    # Restore mode, ownership, and any other metadata for the file.  This is
    # split out from the code above since the code is the same regardless of
    # file type.
    my $mtime = $info{mtime} || time();
    utime time(), $mtime, $dest
        or warn "Unable to update mtime for $dest";

    my $uid = -1;
    my $gid = -1;
    if (defined $info{user}) {
        my @items = split /\s/, $info{user};
        $uid = parse_int($items[0]) if exists $items[0];
    }
    if (defined $info{group}) {
        my @items = split /\s/, $info{group};
        $gid = parse_int($items[0]) if exists $items[0];
    }
    chown $uid, $gid, $dest
        or warn "Unable to change ownership for $dest";

    if (defined $info{mode}) {
        my $mode = parse_int($info{mode});
        chmod $mode, $dest
            or warn "Unable to change permissions for $dest";
    }
}

########################### METADATA LIST PROCESSING ##########################
# Process the file metadata listing provided, and as information for each file
# is extracted, pass it to process_file.  This will recursively follow indirect
# references to other metadata objects.
sub process_metadata {
    my ($metadata, $recursion_level) = @_;

    # Check recursion; this will prevent us from infinitely recursing on an
    # indirect reference which loops back to itself.
    $recursion_level ||= 0;
    if ($recursion_level >= $RECURSION_LIMIT) {
        die "Recursion limit reached";
    }

    # Split the metadata into lines, then start processing each line.  There
    # are two primary cases:
    #   - Lines starting with "@" are indirect references to other metadata
    #     objects.  Recursively process that object before continuing.
    #   - Other lines should come in groups separated by a blank line; these
    #     contain metadata for a single file that should be passed to
    #     process_file.
    # Note that blocks of metadata about a file cannot span a boundary between
    # metadata objects.
    my %info = ();
    my $line;
    my $last_key;
    foreach $line (split /\n/, $metadata) {
        # If we find a blank line or a reference to another block, process any
        # data for the previous file first.
        if ($line eq '' || $line =~ m/^@/) {
            process_file(%info) if %info;
            %info = ();
            undef $last_key;
            next if $line eq '';
        }

        # Recursively handle indirect metadata blocks.
        if ($line =~ m/^@(\S+)$/) {
            print "Indirect: $1\n" if $VERBOSE;
            my $indirect = load_ref($1);
            process_metadata($indirect, $recursion_level + 1);
            next;
        }

        # Try to parse the data as "key: value" pairs of file metadata.  Also
        # handle continuation lines, which start with whitespace and continue
        # the previous "key: value" pair.
        if ($line =~ m/^(\w+):\s*(.*)$/) {
            $info{$1} = $2;
            $last_key = $1;
        } elsif ($line =~/^\s/ && defined $last_key) {
            $info{$last_key} .= $line;
        } else {
            print STDERR "Junk in file metadata section: $line\n";
        }
    }

    # Process any last file metadata which has not already been processed.
    process_file(%info) if %info;
}

############################### MAIN ENTRY POINT ##############################
# Program start.  We expect to be called with a single argument, which is the
# name of the backup descriptor file written by a backup pass.  This will name
# the root object in the snapshot, from which we can reach all other data we
# need.

# Parse command-line arguments.  The first (required) is the name of the
# snapshot descriptor file.  The backup objects are assumed to be stored in the
# same directory as the descriptor.  The second (optional) argument is the
# directory where the restored files should be written; it defaults to ".";
my $descriptor = $ARGV[0];
unless (defined($descriptor) && -r $descriptor) {
    print STDERR "Usage: $0 <snapshot file>\n";
    exit 1;
}

if (defined($ARGV[1])) {
    $DEST_DIR = $ARGV[1];
}

$OBJECT_DIR = dirname($descriptor);
print "Source directory: $OBJECT_DIR\n" if $VERBOSE;

# Read the snapshot descriptor to find the root object.  Parse it to get a set
# of key/value pairs.
open DESCRIPTOR, "<", $descriptor
    or die "Cannot open backup descriptor file $descriptor: $!";
my %descriptor = ();
my ($line, $last_key);
while (defined($line = <DESCRIPTOR>)) {
    # Any lines of the form "key: value" should be inserted into the
    # %descriptor dictionary.  Any continuation line (a line starting with
    # whitespace) will append text to the previous key's value.  Ignore other
    # lines.
    chomp $line;

    if ($line =~ m/^(\w+):\s*(.*)$/) {
        $descriptor{$1} = $2;
        $last_key = $1;
    } elsif ($line =~/^\s/ && defined $last_key) {
        $descriptor{$last_key} .= $line;
    } else {
        undef $last_key;
        print STDERR "Ignoring line in backup descriptor: $line\n";
    }
}

# A valid backup descriptor should at the very least specify the root metadata
# object.
if (!exists $descriptor{Root}) {
    die "Expected 'Root:' specification in backup descriptor file";
}
my $root = $descriptor{Root};
close DESCRIPTOR;

# Set the umask to something restrictive.  As we unpack files, we'll originally
# write the files/directories without setting the permissions, so be
# conservative and ensure that they can't be read.  Afterwards, we'll properly
# fix up permissions.
umask 077;

# Start processing metadata stored in the root to recreate the files.
print "Root object: $root\n" if $VERBOSE;
my $contents = load_ref($root);
process_metadata($contents);
