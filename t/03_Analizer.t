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

ok($uniq, "get_attributes() : $uniq");


ok(scalar @$uniq > 10 ,'get_attributes() returns element ammount we expected') or die;

my $attribute_counts = $a->get_attributes_counts;
### $attribute_counts

my $ratios = $a->get_attributes_ratios;
### $ratios

my $cratios = $a->get_attributes_by_ratio;
### $cratios






for my $att (@$cratios){

   my $options = $a->attribute_option_list($att) or next;
   print STDERR " $att : @$options\n";  

}


$a->dbh->disconnect;




# ------------------------------

ok(1,"\n\n\n# # # PART 2 # # # \n\n");
require Metadata::DB::Search::InterfaceHTML;
my $g = Metadata::DB::Search::InterfaceHTML->new({ DBH => _get_new_handle() });


ok( $g->tmpl, 'got tmpl()') or die;


ok( scalar @{$g->search_attributes_selected},'search attributes selected has a count') or die; 


ok( $g->generate_search_interface_loop, 'can generate default loop') or die;
ok( scalar @{ $g->generate_search_interface_loop }, 'can generate default loop WITH content inside') or die;


ok( save_form_html($g,'form_default' => $g->html_search_form_output ) );



ok(1,"\n\n\n# # # PART 3 # # # \n\n");
# choose our own ...
# SET LARGER LIMIT
$g->attribute_option_list_limit_set(400);

ok( save_form_html($g,'form_nolimit' => $g->html_search_form_output) );















exit;

sub save_form_html {
   my($obj,$name,$output) = @_;
   

   my $abs_html = cwd()."/t/$name.html";
   
   open(FILE,'>',$abs_html) or die;
   print FILE $output;
   close FILE;
   ok(-f $abs_html, "saved $abs_html");
   return $abs_html;
   
}
