use ExtUtils::MakeMaker;
WriteMakefile(
   NAME => 'Metadata::DB',
   VERSION_FROM  => 'lib/Metadata/DB.pm',
   PREREQ_PM => {
   'HTML::Template' => 0,
   'HTML::Template::Default' => 0,
      'Metadata::Base' => 0,
      'LEOCHARRE::Class' => 0,
      'LEOCHARRE::Class::Accessors' => '1.12',
      'LEOCHARRE::Database' => '1',
      'LEOCHARRE::DEBUG',
   },
);
