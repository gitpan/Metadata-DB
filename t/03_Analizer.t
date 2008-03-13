use Test::Simple 'no_plan';
require './t/testlib.pl';
use strict;
use lib './lib';
use Metadata::DB::Analizer;
use Cwd;
use Metadata::DB;

require Cwd;
#my $abs_db = Cwd::cwd().'/t/test.db';
#unlink $abs_db;
use File::Copy;
File::Copy::cp('./t/dames.db','./t/test.db');
use Smart::Comments '###';

$Metadata::DB::Analizer::DBUG = 1;
# make sure db is setup
my $n = Metadata::DB::Analizer->new({ DBH => _get_new_handle() });
ok($n->table_metadata_check,'table metadata check');
$n->dbh->disconnect;
undef $n;



# getn some garble.
#_gen_people_metadata();




ok(1,'testing ratios and inspect of metadata table..');


# waht have we in there..

my $a = Metadata::DB::Analizer->new({ DBH => _get_new_handle() });




$Metadata::DB::_Base::DEBUG = 1;
   my $dumpp = $a->table_metadata_dump(100);
   print STDERR " DUMP \n$dumpp\n";






$Metadata::DB::Analizer::DEBUG = 1;

my $uniq = $a->get_attributes;
ok(ref $uniq eq 'ARRAY','get_attributes() returns array ref') or die;

ok($uniq, "get_attributes() : @$uniq");


ok(scalar @$uniq > 10 ,'get_attributes() returns element ammount we expected') or die;

my $attribute_counts = $a->get_attributes_counts;
### $attribute_counts

my $ratios = $a->get_attributes_ratios;
### $ratios

my $cratios = $a->get_attributes_by_ratio;
### $cratios




my $count = $a->get_records_count;

ok($count," have $count records");







