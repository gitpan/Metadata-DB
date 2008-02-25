use Test::Simple 'no_plan';
require './t/testlib.pl';
use strict;
use lib './lib';
use Smart::Comments '###';
use Metadata::DB::Search;
use Metadata::DB;
use Cwd;

$Metadata::DB::Search::DEBUG = 1;

#$Metadata::DB::Search::Constriction::DEBUG = 1;
#_gen_people_metadata(200000);


my $dbh = _get_new_handle(cwd().'/t/dames.db');
ok($dbh,'got dbh') or die;


my $s = Metadata::DB::Search->new({ DBH => $dbh });
ok($s,'instanced');





# ---- search 

$s->search({
age => 18,
eyes => 'hazel',
cup => 'A',
});


my $hits = $s->ids_count;
ok($hits, "got $hits");

my $got =0;
for my $id (@{$s->ids}){
   #my $m = get_one($id);
   my $m = Metadata::DB->new({ DBH => $dbh, id => $id });
   
   ok( $m->load, "got ".$m->get('name') );
   
   ok( $m->get('age') == 18, 'param 1' );
   ok( $m->get('eyes') eq 'hazel' , 'param 2');  
   ok( $m->get('cup') eq 'A', 'param 3');
   $got++;
}

my $idco = $s->ids_count;

ok($got == $s->ids_count, "got[$got] and id[$idco] count match") or die;

my $cks;
ok( $cks = $s->constriction_keys,'got constriction keys') or die;
print STDERR "   # constriction keys : @$cks\n";


# ---- search 

$s->search({
age => 15,
hair => 'blonde',
eyes => 'green',
});

for my $id (@{$s->ids}){
   my $m = Metadata::DB->new({ DBH => $dbh, id => $id });
   ok( $m->load, "got ".$m->get('name') );
   ok( $m->get('age') == 15, 'param 1' );
   ok( $m->get('hair') eq 'blonde' , 'param 2');
    
}



my $m = Metadata::DB->new({ DBH => $dbh, id => ($s->ids->[1]) });
$m->load;
my $meta = $m->get_all;

ok( scalar keys %$meta,"get_all returns after load");
### $meta





# ONE MORE SEARCH...
#

# ---- search 

$s->search({
   age => [16,15,17],
   hair => ['blonde','redhead'],
   eyes => 'green',
});

for my $id (@{$s->ids}){
   my $m = Metadata::DB->new({ DBH => $dbh, id => $id });
   ok( $m->load, "got ".$m->get('name') );
   my $hair = $m->get('hair');
   my $age = $m->get('age');
   print STDERR " age $age, hair $hair\n"; 
}














