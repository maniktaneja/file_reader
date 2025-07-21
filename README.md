# file_reader
Bash script based file lister and reader

Run the following command to get a list of files that need to be read
```
./file_lister.sh /<nfs_share_path> output.txt
```
File lister using rsync to fecth the list of files present on the mounted NFS share

The execute file_reader_v2 to fetch each of the files in parallel using the dd command 

```
/file_reader_v2.sh  output.txt -j 32  -b 128K -s
```
