package Metadata::DB::Base;
use strict;
use LEOCHARRE::Class::Accessors single => [qw(
table_metadata_name
table_metadata_column_name_id
table_metadata_column_name_key 
table_metadata_column_name_value
dbh
)];
use LEOCHARRE::DEBUG;
use LEOCHARRE::DBI;
use warnings;
use Carp;

no warnings 'redefine';


sub dbh {
   my $self = shift;
   unless( $self->dbh_get ){
      $self->{DBH} or confess('DBH argument must be provided to constructor.');  
      $self->dbh_set($self->{DBH});
   }
   return $self->dbh_get;
}


# 1) set defaults on load

sub table_metadata_name {
   my $self = shift;
   unless( $self->table_metadata_name_get ){
      $self->table_metadata_name_set('metadata');
   }
   return $self->table_metadata_name_get;
}

sub table_metadata_column_name_id {
   my $self = shift;
   unless( $self->table_metadata_column_name_id_get ){
      $self->table_metadata_column_name_id_set('id');
   }
   return $self->table_metadata_column_name_id_get;
}

sub table_metadata_column_name_key {
   my $self = shift;
   unless( $self->table_metadata_column_name_key_get ){
      $self->table_metadata_column_name_key_set('mkey');
   }
   return $self->table_metadata_column_name_key_get;
}

sub table_metadata_column_name_value {
   my $self = shift;
   unless( $self->table_metadata_column_name_value_get ){
      $self->table_metadata_column_name_value_set('mval');
   }
   return $self->table_metadata_column_name_value_get;
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
     ."  %s varchar(16),\n"
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

sub _record_entries_hashref {
   my($self,$id)=@_;
   defined $id or croak('missing id');
   
   my $meta={};

   $self->{_selectall_id} ||=
       $self->dbh->prepare(
         sprintf
         "SELECT %s,%s FROM %s WHERE %s = ?",
         $self->table_metadata_column_name_key,
         $self->table_metadata_column_name_value,
         $self->table_metadata_name,
         $self->table_metadata_column_name_id,
      );

   $self->{_selectall_id}->execute($id);

   while( my @row = $self->{_selectall_id}->fetchrow_array ){
      my($key,$val) = @row;
      push @{$meta->{$key}}, $val;
   }
   if(DEBUG){
      my @e = keys %$meta;
      debug("got elements[@e]\n");
   }
   $self->{_selectall_id}->finish;

   return $meta;
}

sub _table_metadata_insert {
   my($self,$id,$key,$val)=@_;
   defined $val or confess('missing value arg');

   unless ( $self->{_table_metadata_insert} ){
   
      my $q = sprintf 
         "INSERT INTO %s (%s,%s,%s) values (?,?,?)",
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
   
   ATTRIBUTE : for my $att ( keys %$meta_hashref){
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



