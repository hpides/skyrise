#!/bin/sh
display_usage() { 
    echo "This script removes shared libraries from a cloud function package that "
    echo "are already present in the AWS Lambda execution environment." 
    echo " Usage:" 
    echo "  ./strip_function_package.sh [-q] name-of-zip-file"
}

BASEDIR=$(dirname "$0")
QUIET=false

if [ "$1" = "-q" ]; then
    QUIET=true
    shift
fi

if [ $# -lt 1 ]; then 
    display_usage
    exit 2
fi 

if [ ! -f "$1" ]; then
    echo "Error: Package does not exist."
    exit 1
fi

if [ $QUIET = false ]; then
    size_before=`du -h $1`
    echo "Before: $size_before"
fi

# A list of all shared libraries in the AWS Lambda execution environment can be obtained with something like:
#  docker run --rm --entrypoint /bin/sh lambci/lambda:provided -c "ls /lib64 /usr/lib64 | grep \.so"
list=`tail -n +2 $BASEDIR/shared_libraries_in_runtime.txt`

prefix_list=""
for lib in $list; do
    prefix_list="$prefix_list lib/$lib"
done

zip -q -d $1 $prefix_list

if [ $QUIET = false ]; then
    size_after=`du -h $1`
    echo " After: $size_after"
fi
