package Metadata::DB::Analizer;
use strict;
use Carp;
use warnings;
use LEOCHARRE::DEBUG;
use base 'Metadata::DB::Base';
use LEOCHARRE::Class::Accessors 
   multi => ['search_attributes_selected','_attributes'], single => ['attribute_option_list_limit'];

no warnings 'redefine';
sub search_attributes_selected {
   my $self = shift;
   
   unless( $self->search_attributes_selected_count ){
      debug("no search attributes list has been selected, we will chose all");
      my @params = sort @{ $self->get_attributes };
      debug("params are [@params]\n");
      for (@params){
         $self->search_attributes_selected_add($_);
      }
   }

   # weed out ones that have one option only? maybe not since the alternative is none..
   # because one option for an att, the alternative is still valid.
   

   return $self->search_attributes_selected_arrayref;
}




# INTERFACE

# for each of the attributes, how many variations are there?
# if there are less then x, then make it a drop down box

# this is not meant to be used online, only offline, as a regeneration of query interface


# if they are more then x choices, then return false
# what we used this for is making a drop down 
sub attribute_option_list {
   my($self,$attribute,$limit) = @_;
   
   defined $attribute or croak('missing dbh or attribute name');
   $limit ||= $self->attribute_option_list_limit;
  
   # order it 
   my $list = $self->attribute_all_unique_values($attribute,$limit) or return;

   my $sorted = _sort($list);
   
    # unshift into list a value for 'none' ?
   
   
   return $sorted;
}

sub attribute_option_list_limit {
   my $self = shift;
   unless( $self->attribute_option_list_limit_get ){   
      $self->attribute_option_list_limit_set(15);
   }
   return $self->attribute_option_list_limit_get;
}



sub _sort {
   my $list = shift;
   
   for(@$list){
      $_=~/^[\.\d]+$/ and next;
      
      # then we are string!      
      return [ sort { lc $a cmp lc $b } @$list ];      
   }
   
   # we are number
   return [ sort { $a <=> $b } @$list ];   
}


#just for heuristics!!! not accurate!
sub _att_uniq_vals {
   my ($self,$att) = @_;
   defined $att or croak('missing att');

   my $limit = 1000;

   # unique vals
   my $s = $self->dbh->prepare_cached(
      sprintf "SELECT DISTINCT %s FROM %s WHERE %s=? LIMIT ?", # this is for heuristics
      $self->table_metadata_column_name_value,
      $self->table_metadata_name,
      $self->table_metadata_column_name_key
      );
   
   $s->execute($att,$limit);
   
   my $value;
   $s->bind_columns(\$value);

   my @vals;
   while($s->fetch){
      push @vals,$value;
   }
   return \@vals;
}




sub attribute_all_unique_values {
   my ($self,$attribute,$limit) = @_;
   defined $attribute or croak('missing dbh or attribute name');
   
   my $_limit;
   if(defined $limit){
      $_limit = ' LIMIT '.($limit+1);
   }
   else {
      $_limit = '';
   }

   debug("limit = $limit\n") if $limit;
   
   

   # unique vals
   my $q = sprintf "SELECT DISTINCT %s FROM %s WHERE %s='%s' $_limit",
   
   $self->table_metadata_column_name_value,
   $self->table_metadata_name,
   $self->table_metadata_column_name_key,
   $attribute,
   ;
   
   
   #   debug(" query: $q \n");
   
   my $r = $self->dbh->selectall_arrayref($q);
   
   my @vals = ();
   for(@$r){
      push @vals, $_->[0];
   }         

   if(scalar @vals and $limit and (scalar @vals > $limit)){
      debug("limit [$limit] exceeded, try higher limit?\n");
      return;
   }
   return \@vals;
}


# pass it one attribute name, tells how many there are (possibilities) distinct values
sub attribute_all_unique_values_count { # THIS WILL BE SLOW
   my ($self,$attribute) =@_;
   defined $attribute or confess('missing attribute arg');

   my $vals = $self->attribute_all_unique_values($attribute);
   my $count = scalar @$vals;
   return $count;

}

sub attribute_type_is_number {
   my ($self,$att) = @_;
   defined $att or croak('missing attribute name');

   my $vals = $self->_att_uniq_vals($att) or return;
   scalar @$vals or return;
   for (@$vals){
      /^\d+$/ or return 0;      
   }
   return 1;
}


# multi
*get_attributes = \&_attributes;
sub _attributes {
   my $self = shift;
   unless( $self->_attributes_count ){
      debug('_attributes_count returned none.. ');
      my $atts = $self->_distinct_attributes_arrayref;
	ref $atts eq 'ARRAY' or die('not array ref');
      debug("got atts scalar: [".scalar @$atts."]");
      for(@$atts){
         $self->_attributes_add($_);
      }
         
   }
   return $self->_attributes_arrayref;
}

sub _distinct_attributes_arrayref {
   my $self = shift;
   
   my $keys = $self->dbh->selectcol(
      sprintf
      "SELECT DISTINCT %s FROM %s",
      $self->table_metadata_column_name_key,
      $self->table_metadata_name,
      );
      debug("keys @$keys\n");
   return $keys;
}




# get ratio of attributes, how many 'age', 'name', and 'color' etc atts are there

sub get_attributes_ratios {
   my $self = shift;

   my $at = $self->get_attributes_counts;
   
   $at->{all} or croak('no atts in table ?');

   my $attr ={};

   for my $att ( keys %$at){      

      # total entries
      $attr->{$att} = 
         int (($at->{$att} * 100) / $at->{all} );      
      
   }

   delete $attr->{all};
   return $attr;
}

sub get_attributes_by_ratio {
   my $self = shift;
   

   my $_att = $self->get_attributes_ratios;

   my @atts = sort { $_att->{$b} <=> $_att->{$a} } keys %$_att;
   return \@atts;
}


sub get_attributes_counts {
   my $self = shift;


   my $attr ={};
   my $_atts = $self->get_attributes;

   my $total=0;
   
   for my $att (@$_atts){      

      # total entries
      $attr->{$att} = $self->attribute_all_unique_values_count($att);      
      $total+= $attr->{$att};
   }

   # actaully we can just add all the vals, can get diff numb.. but.. whatever- not urgent.
   $attr->{ all } = $total; #$self->dbh->rows_count($self->table_metadata_name);

   return $attr;
      

}








1;

=pod

=head1 NAME

Metadata::DB::Analizer - subs to genereate search interface to metadata table automatically 

=head1 DESCRIPTION

These subs help analize a table in a database about metadata.
They are meant to help create an interface to search the results.

Imagine you are storing metadata about people, you have things like first_name, last_name, etc.

This module will help create the interface, by analizing the table data.

For example, if you add 'age' attribute to the table, and there are a finite number of unique values, 
this code suggests whether to add a drop down select box (in a web interface, for example) or a search text field.

No subroutines are exported by default.

=head1 CAVEATS

These are meant to be used offline, they can use up cpu like mad.
Consider caching the values with Cache::File

=head1 DATABASE LAYOUT

Please see Metadata::DB.




=head1 FUNCTIONS FOR ALL ATTRIBUTES

To inspect the metadata table's contents.

=head2 get_attributes()

returns 'all' of the attributes in the metadata table as array ref.

If you store 'age', 'phone', 'name' in your table, this returns those labels.
This is the basis of the idea here, that if you add another attribute, the search interface will
automatically offer this as a search option.

This is called internally by search_attributes_selected() if you dont select your own out of the list.
this only means what attributes to OFFER the user to search by

=head2 get_attributes_by_ratio()

returns array ref of attributes, sorted by occurrences of that value.
In the above example, if there are 100 'name' entries and 8 'phone' entries, the name is closer to 
the front of the list.

=head2 get_attributes_ratios()

returns hash ref. keys are the attribute names (vals in mkey)- values are the percentage
of occurrence, as compared to all the entries.

=head2 get_attributes_counts()

returns hash ref. Each key is an attribute, the values are the number of occurrences.






=head1 FUNCTIONS FOR ONE ATTRIBUTE

Once you know your attribute label/name.

=head2 attribute_option_list()

argument is name of attribute, optional arg is a limit number
the default limit is 15.
returns a list suitable for a select box, or returns undef if the unique values
found for this attribute exceed the limit.

For example, if you have an attribute called hair_color, you can have blonde, brunette, redhead etc.
You would want to offer this as a select box. Thus, if you have blondes and brunnettes as values
for the attribute 'hair_color' in the metadata table..

   my $options = generate_attribute_option_list($dbh,'hair_color');

   # $options = [qw( blonde redhead brunette )];

Note that if your metadata table does not have any entries such as

   id mkey        mval
   1  hair_color  auburn

Then the hair color auburn will not appear in the array ref returned.
Furthermore if there are more then 15 variations of hair color, undef is returned.
If you want to allow for more variations...

For example, if you want to list every single 'first_name' attribute as an option, regardless of how many 
there are..
   
   my $options = generate_attribute_option_list($dbh,'first_name',1000000);

Remember that the return values depend on what the database table holds!

=head2 attribute_option_list_limit()

returns defatult limit set. by default this is 15.
if an attribute to be selected from has more then this count, it is offered as a field,
if it has less, it is a drop down box.
this can be overridden on a per attribtue basis also, this is just the main default

=head2 attribute_option_list_limit_set()

set the limit for most options avail before we show a text field instead of a drop down
default is 15

=head2 attribute_all_unique_values()

argument is dbh and atrribtue name. optional arg is limit (default is 15)
returns array ref.

if you provide the limit, and it is reached (more then 'limit' unique value occurrences) then it returns undef.


=head2 attribute_type_is_number()

argument is the attribute name
analizes the possible values and determines if they are all numbers
returns boolean

this is useful if you want to offer 'less than' option in a select box, for example






=head1 WHAT SEARCH ATTRIBUTES TO SEARCH BY

When we generate automatic interfaces for searching the metadata.

=head2 search_attributes_selected()

returns list of attributes that will be used in the html interface
if you do not pass a list, all attribtues are chosen
you do not need to specify what kind of selection this is, drop down or text,
the data within the databse will figure it out

this is used by generate_search_interface_loop(), in turn used by html_search_form_output().

So, if you wanted to change what shows up..

   my $i = Metadata::DB::Search::InterfaceHTML({ DBH => $dbh });

   $i->search_attributes_selected_clear;
   $i->search_attributes_selected_add('age','height','name','office');

This means if there are fewer then x 'age' possible values, a dropdown box is generated, etc.
This is also the order.


=head2 search_attributes_selected_clear()

take out all search attributes to 0

=head2 search_attributes_selected_count()

=head2 search_attributes_selected_add()

arg is attribute name

=head2 search_attributes_selected_delete()

arg is attribute name. will take out of list, when generating, will not show up.










=head1 SEE ALSO

Metadata::DB::Search::InterfaceHTML

=head1 AUTHOR

Leo Charre

=cut
