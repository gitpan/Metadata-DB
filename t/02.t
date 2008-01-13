use Test::Simple 'no_plan';
use strict;
use lib './lib';
use Metadata::DB;
use Cwd;
$Metadata::DB::Object::DEBUG = 1;
use Smart::Comments '###';


require Cwd;
my $abs_db = Cwd::cwd().'/t/test.db';



my $dbh = _get_new_handle();




my $id = 1;
my @inv = qw(mice cars computers food clothes hands people legs shoes);

for my $name ( qw(James Larry Barry Mika Laurie Jeanie Larissa Miranda Joseph)){
   
   my $m = Metadata::DB->new({ DBH => $dbh }); 
   $m->table_metadata_check;
   
   $m->id_set($id++);
   
   my $meta = { 
      name => $name,
      age => ( int rand 12 ) + (int rand 12),
      inventory =>  $inv[(int rand $#inv)+1] ,      
      vin => int rand (20000) + 130,
   };

   $m->add( %$meta );

   $m->save;
} 






# ok... now what....

# oh yeah.. load the shit

# kill the dbh 

undef $dbh;

ok(1,'undef dbh');

my $db =  _get_new_handle();
ok($db,"new db handle");


$Metadata::DB::DEBUG = 1;

my $d = Metadata::DB->new({ DBH => $db, id=>3 });
ok($d->load,'load'); # HAVE TO LOAD

my $entries_count = $d->entries_count;


ok($d->id_exists, 'id exists, thus in db');
ok($d->id, 'id is set');

ok($entries_count, "entries = $entries_count");

ok( $d->get('name') eq 'Barry', 'name meta is Barry');



### $d
















# make a buncha entries


sub _get_new_handle {
   
   
   my $dbh = DBI::connect_sqlite($abs_db);
   ok( $dbh,'opened dbh with connect_sqlite()') or die;
   return $dbh;


}



