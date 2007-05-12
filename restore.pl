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

my $OBJECT_DIR = ".";           # Directory where objects are unpacked

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
        or die "Unable to open object: $OBJECT_DIR/$segment/$object";
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

############################### MAIN ENTRY POINT ##############################
my $object = $ARGV[0];

#print "Object: $object\n\n";

my $contents = load_ref($object);
print $contents;
