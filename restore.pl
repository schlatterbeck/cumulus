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
# Copyright (C) 2007  Michael Vrable

use strict;
use Digest::SHA1;
use File::Basename;

my $OBJECT_DIR = ".";           # Directory where objects are unpacked
my $RECURSION_LIMIT = 3;        # Bound on recursive object references

############################ CHECKSUM VERIFICATION ############################
# A very simple later for verifying checksums.  Checksums may be used on object
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
    if ($verifier->{HASH} ne $newhash) {
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
    # First, try to parse the object reference string into constituent pieces.
    # The format is segment/object(checksum)[range].  Both the checksum and
    # range are optional.
    my $ref_str = shift;

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
        if ($range !~ m/^\[(\d+)\+(\d+)\]$/) {
            die "Malformed object range: $range";
        }

        my $object_size = length $contents;
        my ($start, $length) = ($1 + 0, $2 + 0);
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
            iterate_objects($callback, $arg, $1, $recursion_level + 1);
        } else {
            &$callback($arg, $obj);
        }
    }
}

sub obj_callback {
    my $verifier = shift;
    my $obj = shift;
    my $data = load_ref($obj);
    print "    ", $obj, " (size ", length($data), ")\n";
    verifier_add_bytes($verifier, $data);
}

sub process_file {
    my %info = @_;

    # TODO
    print "process_file: ", uri_decode($info{name}), "\n";

    if (defined $info{data}) {
        my $verifier = verifier_create($info{checksum});

        iterate_objects(\&obj_callback, $verifier, $info{data});

        print "    checksum: ", (verifier_check($verifier) ? "pass" : "fail"),
            " ", $info{checksum}, "\n";
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
    foreach $line (split /\n/, $metadata) {
        # If we find a blank line or a reference to another block, process any
        # data for the previous file first.
        if ($line eq '' || $line =~ m/^@/) {
            process_file(%info) if %info;
            %info = ();
            next if $line eq '';
        }

        # Recursively handle indirect metadata blocks.
        if ($line =~ m/^@(\S+)$/) {
            print "Indirect: $1\n";
            my $indirect = load_ref($1);
            process_metadata($indirect, $recursion_level + 1);
            next;
        }

        # Try to parse the data as "key: value" pairs of file metadata.
        if ($line =~ m/^(\w+):\s+(.*)\s*$/) {
            $info{$1} = $2;
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

my $descriptor = $ARGV[0];
unless (defined($descriptor) && -r $descriptor) {
    print STDERR "Usage: $0 <snapshot file>\n";
    exit 1;
}

$OBJECT_DIR = dirname($descriptor);
print "Source directory: $OBJECT_DIR\n";

open DESCRIPTOR, "<", $descriptor
    or die "Cannot open backup descriptor file $descriptor: $!";
my $line = <DESCRIPTOR>;
if ($line !~ m/^root: (\S+)$/) {
    die "Expected 'root:' specification in backup descriptor file";
}
my $root = $1;
close DESCRIPTOR;

print "Root object: $root\n";

my $contents = load_ref($root);
process_metadata($contents);
