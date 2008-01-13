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
   
   

   my ($table,$colk,$colv,$coli) = ($self->table_metadata_name, $self->table_metadata_column_name_key, $self->table_metadata_column_name_value, $self->table_metadata_column_name_id);
	my $select= {
	 'like'  => $self->dbh->prepare("SELECT $coli FROM $table WHERE $colk=? and $colv LIKE ?"),
	 'exact' => $self->dbh->prepare("SELECT $coli FROM $table WHERE $colk=? and $colv=?"),
	};	
	my $sk = 'like'; #default

	my $RESULT = {};

	for ( keys %{$arg} ){
		my ($key,$value)= ($_,undef); 
		
		if ($key=~s/:exact$//){ # EXACT, so they can override the like
			$value = $arg->{$_};
			$sk= 'exact';
		}
		else { # LIKE		
			$key=~s/:like$//; # just in case
			$value = "%".$arg->{$_}."%";
			$sk ='like'			
		}
		
		$select->{$sk}->execute($key,$value) or warn("cannot search? $DBI::errstr");

		while ( my $row = $select->{$sk}->fetch ){
			$RESULT->{$row->[0]}->{_hit}++;
		}		
		
	}

	# just leave the result whose count matches num of args?
	# instead should order them to the back.. ?
	my $count = 0;
   my $ids = [];
	for (keys %{$RESULT}){
   
		# not full match? take out
		if( $RESULT->{$_}->{_hit} < $self->search_params_count ){
			delete $RESULT->{$_};
			next;			
		}
      
		#$RESULT->{$_} = $self->get_all($_);
      push @$ids, $_;
		$count++;		
	}
	
	#$self->{_search}->{count} = $count;
	#$self->{_search}->{data}  = $RESULT;
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
   
   for 

=head1 METHODS

=head2 search_params_count()

returns how many search params we have

=head2 search_params_add()

=head2 constriction_keys()
   
=head2 search()

optional argument is a hash ref with search params

=head1 ids()

returns array ref of matching ids, results, in metadata table that meet the criteria

=cut
