package Metadata::DB::Search::InterfaceHTML;
use strict;
use base 'Metadata::DB::Analizer';
use LEOCHARRE::Class::Accessors single => ['tmpl'];
use warnings;
use Carp;
no warnings 'redefine';







# OUTPUT ---------------------------------------

sub cgiapp_search_form_output {
   my ($self,$submit_rm) = @_;

   $submit_rm ||= 'search_results';
   
   my $out = q{<form name="search_form" action="?rm=search_results" method="post">};
   $out .=   $self->html_search_form_output;
   $out .=   qq|<p><input type="submit" value="search"></p><input type="hidden" name="rm" value="$submit_rm"></form>|;
   
   return $out; 
}

sub html_search_form_output {
   my $self = shift;
   $self->tmpl->param( SEARCH_INTERFACE_LOOP => $self->generate_search_interface_loop );
   $self->tmpl->param( SEARCH_INTERFACE_HIDDEN_VARS => $self->generate_hidden_vars  );
   
   my $out = $self->tmpl->output;   
   $out=~s/\n{3,}/\n\n/g;
   return $out;
   
}



# TEMPLATE OBJECT ------------------------------------------

sub tmpl {
   my $self = shift;
   unless( $self->tmpl_get ){
      my $tmpl = _search_tmpl() or die;
      $self->tmpl_set($tmpl);
   }
   return $self->tmpl_get;
}

sub _search_tmpl {
   require HTML::Template::Default;
   my $code = _html_tmpl_code_default();   
   my $tmpl = HTML::Template::Default::get_tmpl('metadata_search_form.html',\$code);
   return $tmpl;
}

sub _html_tmpl_code_default {
   
 my $tmpl = q|
<TMPL_LOOP SEARCH_INTERFACE_LOOP>
 <div><p><b><TMPL_VAR ATTRIBUTE_NAME></b> 




 <TMPL_IF INPUT_TYPE_SELECT>

  <select name="<TMPL_VAR ATTRIBUTE_NAME>">
  <TMPL_LOOP SELECT_OPTIONS>
   <option value="<TMPL_VAR OPTION_VALUE>"><TMPL_VAR OPTION_NAME></option>
  </TMPL_LOOP>
  </select>
  
  <input type="hidden" name="<TMPL_VAR ATTRIBUTE_NAME>_match_type_exact>" value="1">
  
    
 <TMPL_ELSE>

   <input type="text" name="<TMPL_VAR ATTRIBUTE_NAME>">
   <input type="checkbox" name="<TMPL_VAR ATTRIBUTE_NAME>_match_type_exact>"> exact
  
 </TMPL_IF>





  <br><small><TMPL_VAR TOTAL_COUNT></small></p>
 </div>
</TMPL_LOOP>

<TMPL_VAR SEARCH_INTERFACE_ATTRIBUTES>

<TMPL_VAR SEARCH_INTERFACE_HIDDEN_VARS>

|;

   

   return $tmpl;
}






# LOOPS & VARS --------------------------------------------

# this one is auto as far as the ammounts are concerned, the limits
sub generate_search_interface_loop {
   my $self = shift;      
   my @loop = ();   
   for my $attribute_name ( @{$self->search_attributes_selected} ){      
      my $hashref = $self->generate_search_attribute_params($attribute_name);      
      push @loop, $hashref;   
   }
   return \@loop;
}
sub generate_hidden_vars {
   my $self = shift;
   my $html;
   for my $att ( @{$self->search_attributes_selected}){
      $html.=qq{<input type="hidden" name="search_interface_attribute" value="$att">};
   }
   return $html;
}





# make hashref for one search attribute
sub generate_search_attribute_params {
   my($self,$att,$limit) = @_;

   $self->_attributes_exists($att)
      or warn("attribute $att does NOT exist.") # die?
      and return;


   # approximately now many diff vals are there in db for this attr
   my $approx_count = $self->attribute_all_unique_values_count($att);
   my $param = {};
   
   # is this a drop down ????
   if( my $opts = $self->attribute_option_list($att,$limit) ){ 
   
         # first should be blank
         my @opts_loop = ({ option_name => '----', option_value => '' });
                  
         map { push @opts_loop, { option_name => $_, option_value => $_ } } @$opts;
      
         $param = {         
            attribute_name => $att,
            #attribute_options => $opts,    
            select_options => \@opts_loop,
            input_type => 'select',
            input_type_select => 1,
            input_type_text => 0,
            total_count => $approx_count,
         };
         
   }

   #just simple text then
   else { 
      $param =  {
         attribute_name => $att,
         input_type => 'text',
         input_type_select => 0,
         input_type_text => 1,
         total_count => $approx_count,
         
      };      
   }

   return $param;
}




1;

=head1 NAME

Metadata::DB::Search::InterfaceHTML

=head1 DESCRIPTION

This generates html output suitable for a web search interface to the metadata
this is not meant to specifially mean the metadata is about files, people, or anything
this JUST provides an interface to the stuff
this is NOT meant to be used live- this is meant to be used as an update tool only.

This code is separate from Metadata::DB::Analizer, because that code could be used to 
gen interface for tk, etc, any gui.
This module IS specifically for a HTML gui.

This module usea Metadata::DB::Analizer as base.

=head1 METHODS

=head2 html_search_form_output()

returns output with interface, just the form guts

=head2 cgiapp_search_form_output()

returns search form appropriate for CGI::Application usage
optional arg is next runmode name (the runmode receiving the params);



=head2 _html_tmpl_code_default()

returns the HTML::Template code that will be used
you can override this in various ways
see HTML::Template::Default


=head2 tmpl()

returns HTML::Template object.






=head1 GENERATE A SEARCH INTERFACE

You dont *have*to use these. 
These ouptut for HTML::Template loops, params, etc.


=head2 generate_search_attribute_params()

argument is attribute name, and optionally  a limit number
if the attribute does not exist in database, warns and returns undef

returns hash ref suitable for HTML::Template

if your tmpl is:
   
   <TMPL_LOOP SEARCH_OPTS_LOOP>
   
   <div>
    <b><TMPL_VAR ATTRIBUTE_NAME></b>
    
    <TMPL_IF INPUT_TYPE_SELECT>
    
    
         <select name="<TMPL_VAR ATTRIBUTE_NAME>">
          <TMPL_LOOP SELECT_OPTIONS>
           <option value="<TMPL_VAR OPTION_VALUE>"><TMPL_VAR OPTION_NAME></option>
          </TMPL_LOOP>
         </select>
         
    
    <TMPL_ELSE>
    
         <input type="text" name="<TMPL_VAR ATTRIBUTE_NAME>">
    
    </TMPL_IF> 
   </div>
   
   </TMPL_LOOP>
   <TMPL_VAR SEARCH_INTERFACE_HIDDEN_VARS>

The following means that if there are more then 40 name possible values, show a text field,
if less, show a drop down.
For cars, if there are less the 20 choices (possible metadata values for they key 'car'), show
dropdown, else, show text field.
(The default for all of these is 15.)
   

   my $i = Metadata::DB::Search::InterfaceHTML({ DBH => $dbh });
   
1) get the params for the attributes you want
   
   my $name_opt = $i->generate_search_attribute_params('name',40);   
   my $car_opt =  $i->generate_search_attribute_parmas('car',20);

2) build the main search options loop
   
   my @search_opts_loop = [ $name_opt, $age_opt ];

3) feed it to the template
   
   $i->tmpl->param( SEARCH_OPTS_LOOP => \@search_opts_loop ):

4) now get the output, this is the interface you should show the user.
 

   my $output = $i->tmpl->output;
   
   open(FILE,'>','/home/myself/public_html/search_meta.html');
   print FILE $output;
   close FILE;  



=head2 generate_search_interface_loop()

argument is dbh
returns array ref 
each element is a hash ref
the keys are 

   'attribute_name' the label of the attribute, the name
   'select_options' the values to select from, if there were few of them
   'input_type' this holds 'select' or 'text', the type of field suggested in a web interface
   'input_type_text' boolean
   'input_type_select' boolean

Example template:

   <TMPL_LOOP SEARCH_INTERFACE_LOOP>
   <div>
    <b><TMPL_VAR ATTRIBUTE_NAME></b><TMPL_IF INPUT_TYPE_SELECT>
    <select name="<TMPL_VAR ATTRIBUTE_NAME>"><TMPL_LOOP SELECT_OPTIONS>
    <option><TMPL_VAR OPTION_NAME></option></TMPL_LOOP>
    </select><TMPL_ELSE>
    <input type="text" name="<TMPL_VAR ATTRIBUTE_NAME>"></TMPL_IF> 
   </div>
   </TMPL_LOOP>

Usage:

   $tmpl->param( SEARCH_INTERFACE_LOOP => generate_search_interface_loop($dbh) );


=
