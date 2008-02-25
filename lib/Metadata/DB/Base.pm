package Metadata::DB::Base;
use strict;
#use LEOCHARRE::Class::Accessors single => [qw(
#table_metadata_name
#table_metadata_column_name_id
#table_metadata_column_name_key 
#table_metadata_column_name_value
#dbh
#)];
use LEOCHARRE::DEBUG;
use LEOCHARRE::DBI;
use warnings;
use Carp;

sub new {
   my($class,$self) = @_;
   $self||={};
   bless $self,$class;
   return $self;
}


no warnings 'redefine';




sub dbh {
   my $self = shift;
   $self->{DBH} or confess('DBH argument must be provided to constructor.');  
   return $self->{DBH};
}


# 1) set defaults on load

sub table_metadata_name {
   my $self = shift;
   $self->{table_metadata_name} ||= 'metadata';
}

sub table_metadata_column_name_id {
   my $self = shift;
   $self->{table_metadata_column_name_id} ||= 'id';
   return $self->{table_metadata_column_name_id};
}

sub table_metadata_column_name_key {
   my $self = shift;
   $self->{table_metadata_column_name_key} ||= 'mkey';
   return $self->{table_metadata_column_name_key};
}

sub table_metadata_column_name_value {
   my $self = shift;
   $self->{table_metadata_column_name_value} ||= 'mval';
   return $self->{table_metadata_column_name_value};
}

# setup

sub table_metadata_exists {
   my $self = shift;
   return $self->dbh->table_exists($self->table_metadata_name);   
}

sub table_metadata_create {
   my $self = shift;
   my $layout = $self->table_metadata_layout;
   debug("creating table:\n$layout\n");
   $self->dbh->do($layout);
   return 1;
}

sub table_metadata_layout {   
   my $self = shift;
   
   my $current = 
      sprintf
     "CREATE TABLE %s (\n"
    # ."  %s varchar(16),\n" # INSTEAD OF CHAR, USE INT, should be quicker
     ."  %s int,"
     ."  %s varchar(32),\n"
     ."  %s varchar(256)\n"       
     .");\n",
     $self->table_metadata_name,
     $self->table_metadata_column_name_id,
     $self->table_metadata_column_name_key,
     $self->table_metadata_column_name_value     
     ;
   return $current;     
}


# this is mostly debug
sub table_metadata_dump {
   my $self = shift;
   my $limit = shift; # at most x extries??
   
   my $dbh = $self->dbh or die('no dbh() returned');
   if (defined $limit){
      $limit = " LIMIT $limit";
   }
   $limit||='';

   my $q = sprintf "SELECT * FROM %s $limit", $self->table_metadata_name;
   debug("q: $q\n");
   

   my $_dump;

   my $r = $dbh->selectall_arrayref( $q );
   
   my $out;

   my $_id;
   
   for(@$r){
      my ($id,$key,$val) = @$_;
      if(!$_id or ($id ne $_id)){
         $out.="\n$id: ";
         $_id = $id;
      }

      $out.=" $key:$val";
      $_dump->{$id}->{$key} = $val;      
   }
   $out.="\n\n";

   return $out;
 #  require Data::Dumper;
 #   my $string = Data::Dumper::Dumper($_dump);
  # return $string;
}

sub table_metadata_check {
   my $self = shift;
   $self->table_metadata_exists or $self->table_metadata_create;
   return 1;
}


# SINGLE RECORD METHODS, ETC


# how many entries does a record hold in the metadata table
sub _record_entries_count {
   my ($self,$id) = @_;
   defined $id or die('missing id');
   my $count = $self->dbh->rows_count(
      $self->table_metadata_name,
      $self->table_metadata_column_name_id,
      $id,
   );
   return $count;
}

# delete all entries from db for one record
sub _record_entries_delete {
   my ($self,$id)=@_;
   defined $id or croak('missing id arg');
  
	# what if the table is not there?
 
   $self->{_dsth} ||= 
      $self->dbh->prepare( sprintf 
        "DELETE FROM %s WHERE %s=?", 
        $self->table_metadata_name,
        $self->table_metadata_column_name_id);
        
   $self->{_dsth}->execute($id);
   # is do quicker ?

   # TODO,  return count of rows affected??
   return 1;
}


*{_record_entries_hashref} = \&_record_entries_hashref_3; # THIS IS THE BEST ONE


# TODO this needs to be redone to be faster.. somehow
sub _record_entries_hashref_1 {
   my($self,$id)=@_;
   defined $id or croak('missing id');
   
   my $meta={};

   
   unless( $self->{_selectall_id} ){
      my $attribute_return_limit = 100;      
      
      my $prepped = $self->dbh->prepare(
         sprintf 'SELECT %s,%s FROM %s WHERE %s = ? LIMIT %s',
         $self->table_metadata_column_name_key, $self->table_metadata_column_name_value,
         $self->table_metadata_name, $self->table_metadata_column_name_id, $attribute_return_limit
      );
      $self->{_selectall_id} = $prepped;
   }      
  
   $self->{_selectall_id}->execute($id);

   
   while ( my @row = $self->{_selectall_id}->fetchrow_array ){
      push @{ $meta->{$row[0]} }, $row[1];
   }
   if(DEBUG){
      my @e = keys %$meta;
      debug("got elements[@e]\n");
   }
   #$self->{_selectall_id}->finish; # maybe this is what's slowing it down
   # DONT USE finish(), it closes up the statement, means no more will be used of this statement!!!

   return $meta;
}

# attempt at making this faster 2 ..
sub _record_entries_hashref_2 {
   my ($self,$id)=@_;
   defined $id or confess('missing id');   
   
   my $meta ={};
   

      my $sth = $self->dbh->prepare_cached(

         sprintf 'SELECT %s,%s FROM %s WHERE %s = ?',
         $self->table_metadata_column_name_key, $self->table_metadata_column_name_value,
         $self->table_metadata_name, $self->table_metadata_column_name_id
      );
 
   $sth->execute($id);

   
   my ($key,$val);
   # USE BIND COLUMNS, SUPPOSEDLY THE MOST EFFICIENT WAY TO FETCH DATA ACCORDING TO DBI.pm
   $sth->bind_columns(\$key,\$val);

   while( $sth->fetch ){
      push @{$meta->{$key}},$val;
   }
   return $meta;
}


# attempt at making this faster 3 ..
sub _record_entries_hashref_3 {
   my ($self,$id)=@_;
   defined $id or confess('missing id');   
   
   my $meta ={};
   
   my $_limit = 500; # expect at most how much meta
   # actually limit is useless unless it is really reached.. :-(
   
   $self->{_record_entries_hashref_3} ||=   
      $self->dbh->prepare_cached(
         sprintf 'SELECT %s,%s FROM %s WHERE %s = ? LIMIT %s',
         $self->table_metadata_column_name_key, $self->table_metadata_column_name_value,
         $self->table_metadata_name, $self->table_metadata_column_name_id,
         $_limit
      );

   my $sth = $self->{_record_entries_hashref_3} or die;
   
   $sth->execute($id);
   
   my $_rows = $sth->fetchall_arrayref;    

   for ( @$_rows ){
      push @{$meta->{$_->[0]}}, $_->[1];
   }
   
   return $meta;
}






sub _table_metadata_insert {
   my($self,$id,$key,$val)=@_;
   defined $val or confess('missing value arg');

   unless ( $self->{_table_metadata_insert} ){
   
      my $q = sprintf 
         'INSERT INTO %s (%s,%s,%s) values (?,?,?)',
         $self->table_metadata_name,
         $self->table_metadata_column_name_id,
         $self->table_metadata_column_name_key,
         $self->table_metadata_column_name_value;
   
      $self->{_table_metadata_insert} = $self->dbh->prepare( $q );

   }

   
   $self->{_table_metadata_insert}->execute( $id, $key, $val);
   return 1;
}


# inject metadata hashref for an id
sub _table_metadata_insert_multiple {
   my($self,$id,$meta_hashref) = @_;
   ref $meta_hashref eq 'HASH' or croak('missing meta hash ref arg');

   my @atts = keys %$meta_hashref
      or  croak("there are no key value pairs in the hashref");  

   ATTRIBUTE : for my $att ( @atts ){
      my $_val = $meta_hashref->{$att};
      defined $_val or debug("att $att was not defined\n") and next ATTRIBUTE;
      if ( ref $_val eq 'ARRAY' ){
         debug("$att is array ref");
         for ( @$_val ){
            $self->_table_metadata_insert( $id, $att, $_ );
         }
         next ATTRIBUTE;
      }
      elsif( ref $_val ){
         croak('only scalars and array refs supported at this time');
      }
      
      debug("$att is scalar");
      $self->_table_metadata_insert( $id, $att, $_val );    
   }
   return 1;   
}




1;


__END__


=pod

=head1 NAME

Metadata::DB::Base

=head1 CONSTRUCTOR

=head2 new()

argument is hash ref
you must pass DBH, database handle, to the object.
This is a database handle you opened before instancing the object
If you wanted to change the name of the table..

   my $m = Metadata::DB::Base({ DBH => $dbh });
   $m->table_metadata_name_set('other_metadata_table');
   $m->table_metadata_check; # make sure the table is there, if you wanted to setup

   

=head1 SETUP AND DB METHODS

=head2 dbh()

returns database handle
The DBI handle is passed to the CONSTRUCTOR

=head2 table_metadata_exists()

does the metadata table exist or not?
returns boolean. 

=head2 table_metadata_create()

creates metadata table, does not check for existance

=head2 table_metadata_dump()

optional argument is limit
returns debug string with a pseudo metadata table dump, suitable for printing to STDERR
for debug purposes.

=head2 table_metadata_layout()

returns what the metadata table is expected to look according to current params
could be useful if you're having a hard time with all of this.
If you turn DEBUG to on, this is printed to STDERR .

=head2 table_metadata_check()

create table if not exists

=head1 RECORD METHODS

=head2 _record_entries_delete()

arg is id
deletes all metadata entries for this record from metadata table
(does not commit, etc)

=head2 _record_entries_count()

arg is id, returns number of record entries in metadata table

=head2 _table_metadata_insert()

arg is id, key, val

=head2 _table_metadata_insert_multiple()

arg is id and hashref



=head2 _record_entries_hashref()

arg is id
returns hashref

=cut
=head1 SPEEDING UP QUERIES

Make sure you have an index on the metadata table

   CREATE INDEX id_index ON metadata(id);




