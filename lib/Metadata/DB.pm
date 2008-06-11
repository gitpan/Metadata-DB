package Metadata::DB;
use strict;
use LEOCHARRE::DEBUG;
use LEOCHARRE::Class2;
use base 'Metadata::Base';
use base 'Metadata::DB::Base';
use Carp;
use vars qw($VERSION);
$VERSION = sprintf "%d.%02d", q$Revision: 1.12 $ =~ /(\d+)/g;

__PACKAGE__->make_constructor();
__PACKAGE__->make_accessor_setget({
   loaded => undef,
   id => undef,   
});
no warnings 'redefine';

# overriding  Metadata::Base::Write
*write = \&save;
sub save {
   my $self = shift;
   my $id = $self->id or confess('no id is set');

   $self->_record_entries_delete( $id );
   $self->_table_metadata_insert_multiple( $id, $self->get_all );
   
   return $id;
}


#does obj id exist in db
sub id_exists {
   my $self = shift;
   return $self->entries_count;
}

sub entries_count {
   my $self = shift;
   my $id = $self->id or warn("no id is set");
   return $self->_record_entries_count($id);
}


*elements_count = \&Metadata::Base::size;
#sub elements_count {
#   my $self = shift;
#   my @e = $self->elements;
#   my $c = scalar @e;
#   debug("$c\n");
#   return $c;
#}


# take object and return meta hash ref of what it holds
sub _get_meta_from_object {
   my $self = shift;
   
   my $meta = {};
   
   my @elements = $self->elements or return {};
   
   
   my $c = $self->elements_count;
   debug("[count was $c], have: [@elements]\n");

   for my $key (@elements){
      my @values = $self->get($key);
      $meta->{$key} = \@values;
   }
   return $meta;
}
*get_all = \&_get_meta_from_object;


# overriding Metadata::Base::Read
*read = \&load;
sub load {
   my $self = shift;

   my $id = $self->id or confess('cannot load, no id is set, no id was passed as arg');
   #debug('calling clear..');
   #$self->clear;
   $self->loaded(1);

   if ( my $meta= $self->_record_entries_hashref($id) ){
      debug("found meta for: $id");
      $self->add(%$meta);
   }
   return 1;
}


sub add {
   my $self = shift;

   while( scalar @_){
      my($key,$val) = (shift,shift);
      defined $key and defined $val or confess('undefined values');
      #debug("adding $key:$val\n");
      # TODO , what if $val is an array ref?? then... is set just recording ONE ????
      # Metadata::Base says if the value is array ref, then all vals are recorded
      $self->set($key, $val);
   }
   return 1;
}






1;

__END__

=pod

=head1 NAME

Metadata::DB

=head1 SYNOPSIS

   use Metadata::DB;

   my $dbh;
   my $o = new Metadata::DB($dbh);
   $o->load;
   $o->set( name => 'jack' );
   $o->set( age  =>  14 );
   $o->id(4);
   $o->save;


   my $o2 = new Metadata::DB($dbh);
   $o2->id(4);
   $o2->load;
   $o2->get( 'name' );
   $o2->set( 'age' => 44 );


=head2 Loading metadata from db

=over 4

=item via constructor

If you pass the id to the constructor, it will attempt to load from db.

   my $o = new Metadata::DB({ DBH => $dbh, id => 'james' });

=item via methods

You can directly tell it what the id will be , and then request to load.

   my $o = new Metadata::DB({ DBH => $dbh });
   $o->id('james');
   $o->load;

=item checking for record

   my $o = new Metadata::DB({ DBH => $dbh });
   $o->id('james');
   $o->load; # you must call load
   $o->id_exists;

=head1 DESCRIPTION

Inherits Metadata::Base and all its methods.

=head2 new()

Argument is hash ref with at least a DBH argument, which is a database handle.

   my $o = Metadata::DB->new({ DBH = $dbh });

Optional argument is 'id'.

=head2 id()

Perl setget method.
Arg is number.

=head2 id_exists()

Returns boolean.
If the id is in the database, that is- if the record by this id has any entries in the 
metadata table, this returns true.

=head2 entries_count()

Returns number of entries for this id.

=head2 set()

Works like Metadata::Base::set()

   $o->set( name => 'Jack' );


=head2 elements()

See Metadata::Base.

=head2 add()

Works like set(), only you can provide many entries.

   $o->add(
      name => 'this',
      age => 4,
   );


=head2 write(), save()

Save to db. You call this to create a new record as well.
Returns id.

=head2 load(), read()

Will attempt to load from db.
YOU MUST CALL load() to check what is in the database.

=head2 loaded()

Returns boolean
If load() was triggered or called or not.

=head2 get_all()

Returns hashref with all meta.
Will attempt to load from db.

=head2 get()

See Metadata::Base.

   $o->get('name');

=head1 CAVEATS

WARNING
Calling save() before load() will delete all record metadata previously saved.

Delete ALL metadata with id 5 and save only 'name marc'.

   my $m = Metadata::DB->new({ DBH => $dbh, id => 5 });
   $m->set( name=> 'marc' );
   $m->save;

After, this will NOT load the metadata 'name marc':

   my $m = Metadata::DB->new({ DBH => $dbh, id => 5 });
   $m->get( 'name' );

This example WILL load the metadata:

   my $m3= Metadata::DB->new({ DBH => $dbh, id => 5 });
   $m->load;
   $m->get( 'name' );

This example will NOT delete metadata and will add instead:

   my $m = Metadata::DB->new({ DBH => $dbh, id => 5 });
   $m->load;
   $m->set( age => 25 );
   $m->save;

Why? Why not just override get and get all to take care of this?
Wny not just call load() automatically??
Because that's up to you. You MAY want to NOT do this cpu intensive operation.
Maybe you want to insert a million entries really quickly, thus you dont want to load
every time, maybe you already know there is nothing in there.

=head1 SEE ALSO

Metadata::Base
Metadata::DB::Base
Metadata::DB::Indexer
Metadata::DB::Analizer
Metadata::DB::WUI

=head1 AUTHOR

Leo Charre leocharre at cpan dot org

=cut
