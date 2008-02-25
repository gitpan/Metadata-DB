package Metadata::DB::Search;
use strict;
use LEOCHARRE::Class::Accessors 
single => ['search_params','_constriction_object','_hits_by_count','ids','ids_count','__objects'], multi => ['constriction_keys'];
use base 'Metadata::DB::Base';
use LEOCHARRE::DEBUG;
use Carp;

sub new {
   my($class,$self)=@_;
   $self||={};
   bless $self,$class;
   $self->_search_reset;
   return $self;   
}


sub _select_limit {
   my($self,$limit) = @_;
   if(defined $limit){
      $self->{_select_limit} = $limit;
   }
   return $self->{_select_limit};
}





sub _search_reset {
   my $self = shift;
   $self->_constriction_object_clear;
   $self->search_params_clear;
   $self->constriction_keys_clear;
   $self->ids_clear;
   debug();
   return 1;
}





sub constriction_keys {
   my $self = shift;
   
   $self->search_params or die('call search_params_add()');
   unless( $self->constriction_keys_count ){
      map { $self->constriction_keys_add($_) } keys %{$self->search_params};
   }
   return $self->constriction_keys_arrayref;
   
}



sub search_params_add {
   my $self = shift;
   
   $self->search_params or $self->search_params_set({});
   
   while( scalar @_){
      my ($key,$val) = (shift,shift);
      $self->search_params->{$key} = $val;
      
   }
   return 1;
}

sub search_params_count {
   my $self = shift;
   my $c = scalar keys %{$self->search_params};
   return $c;
}


sub search { # multiple key lookup and ranked
	my ($self,$arg) = @_;
   
   if( defined $arg ){
      ref $arg eq 'HASH' or croak('missing arg to search'); 		
   	keys %{$arg} or croak('no arguments, must be hash ref with args and vals');
	   $self->_search_reset;
      $self->search_params_add(%$arg);	      
   }

   else {
      $self->search_params or die('missing search params');
      $arg = $self->search_params;
      
   }
   
   

   my ($table,$colk,$colv,$coli) = ( 
      $self->table_metadata_name, 
      $self->table_metadata_column_name_key, 
      $self->table_metadata_column_name_value, 
      $self->table_metadata_column_name_id );


   my $_select_limit = $self->_select_limit;
   if( $_select_limit ){
      $_select_limit = " LIMIT $_select_limit";
   }
   else {
      $_select_limit ='';
   }

   debug("[select limit: $_select_limit] $table $colk $colv $coli");

   

	my $select= {

	 'like'  => (
      $self->dbh->prepare(
         "SELECT $coli FROM $table WHERE $colk=? and $colv LIKE ? $_select_limit") 
         or die($DBI::errstr)
         ),

	 'exact' => (
      $self->dbh->prepare(
         "SELECT $coli FROM $table WHERE $colk=? and $colv = ? $_select_limit") 
         or die($BI::errstr)
         ),

    'lessthan' => (
      $self->dbh->prepare(
         "SELECT $coli FROM $table WHERE $colk=? and $colv < CAST( ? AS SIGNED ) $_select_limit") 
         or die($DBI::errstr)
         ),

    'morethan' => (
      $self->dbh->prepare(
         "SELECT $coli FROM $table WHERE $colk=? and $colv > CAST( ? AS SIGNED ) $_select_limit") 
         or die($DBI::errstr)
         ),


	};	

	my $RESULT = {};
   


   my @search_terms;   
   # this is to they can search({ key => $array_ref }) as well as regular string
   SEARCH_TERMS: for ( keys %$arg ){
      # TODO, should we sanitize the 'values' ?, like take out non alphanum?
      # because wouldnt %this do a like search??

		my ($key,$_value,$select_type)= ($_,undef,undef); 
      $_value = $arg->{$key};

      # what select query to use?
      if( $key=~s/:(\w+)$// ){
         exists($select->{$1}) or croak("select type $1 does not exist");
         $select_type = $1;
      }
      $select_type ||='like';

      exists $select->{$select_type} or confess("select type $select_type does not exist");
         
      my @vals;
      # are there many values to match or one?
      if (my $ref = ref $_value){
         $ref eq 'ARRAY' or croak('can only accept scalar or an array ref');
         @vals = @$_value;
      }
      else {
         @vals =( $_value );
      }

      for my $rawval ( @vals ){
         if ($select_type eq 'like'){
            push @search_terms, [$key, "\%$rawval\%", $select_type];
         }
         else {
            push @search_terms, [$key, $rawval, $select_type];
         }
         debug(" SEARCH TERM: [$select_type, $key, $rawval]");
      }
      next SEARCH_TERMS;
   }
		
	

   QUERY: for ( @search_terms ){
		my ($key,$value,$select_type)= @$_; 
      defined $key or die("key missing");
      defined $value or die("value missing");
      defined $select_type or die('select type missing');

	   debug(" QUERY : $select_type, $key, $value ..");
      
      my $id;
      my $q = $select->{$select_type};
		$q->execute($key,$value) 
         or warn("cannot search? $DBI::errstr");
      debug("executed.\n");
      
      $q->bind_columns(\$id);
   
		while ( my $row = $q->fetch ){
			$RESULT->{$id}->{_hit}++;
		}		
		next QUERY;
	}



	# just leave the result whose count matches num of args?
	# instead should order them to the back.. ?
	my $count = 0;
   my $ids = [];
	for my $id (keys %{$RESULT}){
   
		# not full match? take out
		if( $RESULT->{$id}->{_hit} < (scalar @search_terms) ){ 
         
			delete $RESULT->{$id};
			next;			
		}
      
      push @$ids, $id;
		$count++;		
	}
	
   debug(sprintf "got %s ids\n",scalar @$ids);
   $self->ids_set($ids);
   $self->ids_count_set( scalar @$ids);

	return $ids;
}




1;

=pod

=head1 NAME

Metadata::DB::Search - search the indexed metadata

=head1 SYNOPSIS

   use Metadata::DB::Search;
   use Metadata::DB;
   
   my $s = Metadata::DB::Search->new({ DBH => $dbh });

   $s->search({
      age => 24,
      'first_name:like' => 'jo',   
      'speed:morethan' => 40,
   });

   $s->ids_count or die('nothing found');

   for(@$ids) {
      my $o = new Metadata::DB({ DBH => $dbh, id => $_ });   
   
   }

=head1 EXAMPLE 2

   
   my $s = Metadata::DB::Search->new({ DBH => $dbh });
   
   $s->search_params_add( age => 24 );
   
   $s->search_params_add( 'first_name:like' =>'jo' );


   
   $s->search;

   my @matching_ids = @{ $s->ids };

   for my $id ( @matching_ids ){
   
      
   }
   
=head1 EXAMPLE 3

What if you want to search other metadata table?

   $s->table_name_metadata
   $s->search({
      age => 24,
      'first_name:like' => 'jo',   
   });
   
   

=head1 METHODS

=head2 search_params_count()

returns how many search params we have

=head2 search_params_add()

=head2 constriction_keys()
   
=head2 search()

optional argument is a hash ref with search params
these are key value pairs
the value can be a string or an array ref

   $s->search({
      age => 25,
      'name:exact' => ['larry','joe']
   });

Possible search types for each attribute are like, exact, morethan, lessthan, default
is like.

=head2 _select_limit()

experimental, arg is number, may help speed up searches if set, possible num is 100?

=head1 ids()

returns array ref of matching ids, results, in metadata table that meet the criteria



=cut
