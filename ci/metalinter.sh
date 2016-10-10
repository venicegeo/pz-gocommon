#!/bin/sh

gometalinter \
--deadline=60s \
--concurrency=6 \
--vendor \
--exclude="exported (var)|(method)|(const)|(type)|(function) [A-Za-z\.0-9]* should have comment" \
./...

#--exclude="comment on exported function [A-Za-z\.0-9]* should be of the form" \
#--exclude="cyclomatic complexity 13 of function createQueryDslAsString" \
#--exclude="cyclomatic complexity 12 of function \(\*Service\)\.GetMessage" \
#--exclude="cyclomatic complexity 11 of function \(\*Client\)\.GetMessages" \
