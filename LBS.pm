=head1 NAME

LBS - Perl interface to Log-Structured Backup stores

=cut

package LBS;

use strict;

BEGIN {
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK);

    # Totally unstable API.
    $VERSION = '0.01';

=head1 SYNOPSIS

    use LBS;

    my $store = new LBS::Store;

=cut

    require Exporter;

    @ISA = qw(Exporter);
    @EXPORT = qw();
    @EXPORT_OK = qw(parse_headers);

=head1 DESCRIPTION

This module makes it easy to write Perl scripts that work with backup snapshots
produced by LBS (the Log-Structured Backup System).  Various LBS utilities will
use it.

=cut

    use Carp qw(carp croak);
}

=head1 CLASSES

=head2 LBS::ChecksumVerifier

=over 4

=item new LBS::ChecksumVerifier ( CHECKSUM )

Parse the checksum string C<CHECKSUM> and return an object which can be used to
verify the integrity of a piece of data.  The data can be fed incrementally to
the returned object, and at the end a call can be made to see if the data
matches the originally-supplied checksum.

=cut

{
    package LBS::ChecksumVerifier;
    use Digest::SHA1;

    sub new {
        my $class = shift;
        my $self = { };
        $self->{CHECKSUM} = shift;

        if ($self->{CHECKSUM} !~ m/^(\w+)=([0-9a-f]+)$/) {
            die "Malformed checksum: $self->{CHECKSUM}";
        }

        my $algorithm = $1;
        $self->{HASH} = $2;
        if ($algorithm ne 'sha1') {
            die "Unsupported checksum algorithm: $algorithm";
        }

        $self->{DIGESTER} = new Digest::SHA1;

        bless $self, $class;
        return $self;
    }

    sub add {
        my $self = shift;
        my $data = shift;
        $self->{DIGESTER}->add($data);
    }

    sub verify {
        my $self = shift;
        my $newhash = $self->{DIGESTER}->hexdigest();
        return ($self->{HASH} eq $newhash);
    }
}

=head2 LBS::Store

=item new LBS::Store ( DIRECTORY )

Construct a new Store object, which is used to get access to segments and
metadata files containing snapshots.  The data may be stored in a local
directory, or might be fetched from a remote server.  The constructor above is
for local access, and takes as a single argument the name of the directory
containing all files.

=item load_ref ( REFSTR )

Load the object contents referenced by the given C<REFSTR>.  This will
automatically validate any object checksums.

=cut

{
    package LBS::Store;
    use File::Temp qw(tempdir);

    my $SEGMENT_PATTERN
        = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';

    sub new {
        my $class = shift;
        my $self = { };
        $self->{DIR} = shift;
        $self->{TMPDIR} = tempdir("lbs.XXXXXX", TMPDIR => 1);

        print STDERR "### Tempdir is $self->{TMPDIR}\n";

        $self->{EXTENSION} = ".bz2";
        $self->{FILTER} = "bzip2 -dc";

        $self->{CACHED} = [ ];

        bless $self, $class;
        return $self;
    }

    sub DESTROY {
        my $self = shift;
        $self->_lru_clean(0);
        print STDERR "### Cleaning temporary directory $self->{TMPDIR}\n";
        system("rm", "-rf", $self->{TMPDIR});
    }

    sub _lru_update {
        my $self = shift;
        my $segment = shift;
        my @cache = @{$self->{CACHED}};
        @cache = grep { $_ ne $segment } @cache;
        push @cache, $segment;
        $self->{CACHED} = [ @cache ];
    }

    sub _lru_clean {
        my $self = shift;
        my $limit = shift;
        $limit = 16 unless defined($limit);

        my @cache = @{$self->{CACHED}};
        while (scalar @cache > $limit) {
            my $segment = shift @cache;
            my $dir = "$self->{TMPDIR}/$segment";
            print STDERR "### Cleaning segment $segment\n";
            system("rm", "-rf", $dir);
        }
        $self->{CACHED} = [ @cache ];
    }

    sub _extract {
        my $self = shift;
        my $segment = shift;

        if (grep { $_ eq $segment } @{$self->{CACHED}}) {
            $self->_lru_update($segment);
            return;
        }

        my $file = "$self->{DIR}/$segment.tar$self->{EXTENSION}";
        die "Can't find segment $file" unless -f $file;

        $self->_lru_clean();
        print STDERR "### Extracting segment $segment\n";
        system("$self->{FILTER} <$file | tar -C $self->{TMPDIR} -xf -");
        $self->_lru_update($segment);
    }

    # Load an object, without any support for object slicing or checksum
    # verification.  This method can be overridden by a subclass, and will be
    # called by the full object reference parser below.
    sub load_object {
        my $self = shift;
        my $segment = shift;
        my $object = shift;

        $self->_extract($segment);
        my $file = "$self->{TMPDIR}/$segment/$object";
        open OBJECT, "<", $file or die "Can't open file $file: $!";
        my $contents = join '', <OBJECT>;
        close OBJECT;

        return $contents;
    }

    sub load_ref {
        my $self = shift;
        my $ref = shift;

        if ($ref !~ m/^([-0-9a-f]+)\/([0-9a-f]+)(\(\S+\))?(\[\S+\])?$/) {
            die "Malformed object reference: $ref";
        }

        my ($segment, $object, $checksum, $range) = ($1, $2, $3, $4);

        my $contents = $self->load_object($segment, $object);

        # If a checksum was specified in the object reference, verify the
        # object integrity by computing a checksum of the read data and
        # comparing.
        if ($checksum) {
            $checksum =~ m/^\((\S+)\)$/;
            my $verifier = new LBS::ChecksumVerifier($1);
            $verifier->add($contents);
            if (!$verifier->verify()) {
                die "Integrity check for object $ref failed";
            }
        }

        # If a range was specified, then only a subset of the bytes of the
        # object are desired.  Extract just the desired bytes.
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

    sub list_segments {
        my $self = shift;
        my %segments = ();
        opendir DIR, $self->{DIR};
        foreach (readdir DIR) {
            if (m/^($SEGMENT_PATTERN)(\.\S+)?$/) {
                $segments{$1} = 1;
            }
        }
        closedir DIR;
        return sort keys %segments;
    }

    sub list_snapshots {
        my $self = shift;
        my @snapshots = ();
        opendir DIR, $self->{DIR};
        foreach (readdir DIR) {
            if (m/^snapshot-(.*)\.lbs$/) {
                push @snapshots, $1;
            }
        }
        closedir DIR;
        return sort @snapshots;
    }

    sub load_snapshot {
        my $self = shift;
        my $snapshot = shift;
        open SNAPSHOT, "$self->{DIR}/snapshot-$snapshot.lbs"
            or return undef;
        my $contents = join '', <SNAPSHOT>;
        close SNAPSHOT;
        return $contents;
    }

    sub list_objects {
        my $self = shift;
        my $segment = shift;
        $self->_extract($segment);
        opendir DIR, "$self->{TMPDIR}/$segment";
        my @objects = grep { /[0-9a-f]{8}/ } readdir(DIR);
        closedir DIR;
        return sort @objects;
    }
}

=head2 LBS::MetadataReader

=item new LBS::MetadataReader ( STORE, REF )

=cut

{
    package LBS::MetadataReader;

    sub new {
        my $class = shift;
        my $self = { };

        $self->{STORE} = shift;
        my %args = @_;

        $self->{SPLIT_PATTERN} = $args{SPLIT} || '(?<=\n)';

        bless $self, $class;

        if (exists $args{REF}) {
            $self->{DATA} = [ $self->_read($args{REF}) ];
        } elsif (exists $args{DATA}) {
            my $pattern = $self->{SPLIT_PATTERN};
            $self->{DATA} = [ split /$pattern/, $args{DATA} ];
        } else {
            die "Must specify REF or DATA argument!";
        }

        return $self;
    }

    sub _read {
        my $self = shift;
        my $ref = shift;
        my $pattern = $self->{SPLIT_PATTERN};
        print STDERR "### Reading from $ref\n";
        my @pieces = split /$pattern/, $self->{STORE}->load_ref($ref);
        return @pieces;
    }

    # FIXME: Bound recursion.
    sub get {
        my $self = shift;

        # End of input?
        if (!@{$self->{DATA}}) {
            return undef;
        }

        my $item = shift @{$self->{DATA}};

        # Check for indirect references
        if ($item =~ m/^@(\S*)/) {
            unshift @{$self->{DATA}}, $self->_read($1);
            return $self->get();
        } else {
            return $item;
        }
    }
}

=head2 LBS::MetadataParser

=item new LBS::MetadataParser ( STORE, REF )

=cut

{
    package LBS::MetadataParser;

    sub new {
        my $class = shift;
        my $self = { };

        $self->{STORE} = shift;
        my $ref = shift;
        $self->{READER} = new LBS::MetadataReader $self->{STORE}, REF => $ref;

        bless $self, $class;
        return $self;
    }

    sub get_item {
        my $self = shift;
        my %info = ();
        my $line;
        my $last_key;

        $line = $self->{READER}->get();
        chomp $line if defined $line;
        while (defined($line) && $line ne "") {
            if ($line =~ m/^(\w+):\s*(.*)$/) {
                $info{$1} = $2;
                $last_key = $1;
            } elsif ($line =~/^\s/ && defined $last_key) {
                $info{$last_key} .= $line;
            } else {
                print STDERR "Junk in file metadata section: $line\n";
            }

            $line = $self->{READER}->get();
            chomp $line;
        }

        # Perform a bit of post-processing on the "data" field, which might
        # contain indirect references to blocks.  Pull all the references
        # inline.
        if (exists $info{data}) {
            my $reader = new LBS::MetadataReader($self->{STORE},
                                                 DATA => $info{data},
                                                 SPLIT => '\s+');
            my @blocks = ();
            while (($_ = $reader->get())) {
                push @blocks, $_;
            }
            $info{data} = join " ", @blocks;
        }

        # Don't return an empty result unless we've hit end-of-file.
        if (!%info && defined($line)) {
            return $self->get_item();
        }

        return %info;
    }
}

# Parse an RFC822-style list of headers and return a dictionary with the
# results.
sub parse_headers {
    my $data = shift;
    my %info = ();
    my $line;
    my $last_key;

    foreach $line (split /\n/, $data) {
        if ($line =~ m/^(\w+):\s*(.*)$/) {
            $info{$1} = $2;
            $last_key = $1;
        } elsif ($line =~/^\s/ && defined $last_key) {
            $info{$last_key} .= $line;
        } else {
            undef $last_key;
            print STDERR "Ignoring line in backup descriptor: $line\n";
        }
    }

    return %info;
}

1;
