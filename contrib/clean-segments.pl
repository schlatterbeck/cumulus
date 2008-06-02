#!/usr/bin/perl -w
#
# Garbage collect segments in LBS snapshot directories.
#
# Find all segments which are not referenced by any current snapshots and print
# out a listing of them so that they can be deleted.
#
# Takes no command-line arguments, and expects to be invoked from the directory
# containing the snapshots.
#
# Copyright (C) 2007  Michael Vrable

use strict;

my $SEGMENT_PATTERN
    = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';

# Set of all segments which are used by current snapshots.  Value is ignored.
my %segments_used = ();

# Iterate through all snapshots.  Snapshot descriptors should end with a ".lbs"
# extension.  Find all segments which are used.
foreach (glob "*.lbs") {
    open DESCRIPTOR, "<", $_
        or die "Cannot open backup descriptor file $_: $!";

    # Parse the backup descriptor file.  We might not need the full parser, but
    # it shouldn't hurt.
    my %descriptor = ();
    my ($line, $last_key);
    while (defined($line = <DESCRIPTOR>)) {
        chomp $line;

        if ($line =~ m/^([-\w]+):\s*(.*)$/) {
            $descriptor{$1} = $2;
            $last_key = $1;
        } elsif ($line =~/^\s/ && defined $last_key) {
            $descriptor{$last_key} .= $line;
        } else {
            undef $last_key;
            print STDERR "Ignoring line in backup descriptor: $line\n";
        }
    }

    # Extract the list of segments from the parsed descriptor file.
    foreach (split /\s+/, $descriptor{Segments}) {
        next unless $_;
        if (m/^$SEGMENT_PATTERN$/) {
            $segments_used{$_} = 1;
        } else {
            warn "Invalid segment name: '$_'\n";
        }
    }

    close DESCRIPTOR;
}

# Look for all segments in this directory, and match them against the list
# generated above of segments which are used.  Pring out any segments which are
# not used.
my %segments_found = ();

foreach (glob "*") {
    if (m/^($SEGMENT_PATTERN)(\.\S+)?$/) {
        $segments_found{$1} = 1;
        if (!exists $segments_used{$1}) {
            print $_, "\n";
        }
    }
}

# Perform a consistency check: were any segments referenced by snapshot but not
# found in the directory?
foreach (sort keys %segments_used) {
    if (!exists $segments_found{$_}) {
        print STDERR "Warning: Segment $_ not found\n";
    }
}

