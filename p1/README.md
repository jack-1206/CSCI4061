test machine : csel-kh1250-35.cselabs.umn.edu
date : 09 / 28 / 20
name : Timothy Kotten , Brett Bodnar , Mouhari Mouhamed
x500 : kotte013 , bodna019 , mouha003

Purpose:
    Count unique words in a file, and tally up the number of occurrences.

How to compile:
    Run 'make' in the main directory.

What the program does:
    The parent creates children that run a mapper program that tally up the
    number of word occurrences for each unique word in a chunk of the file
    and write it to a text file. Then the parent creates children that run
    a reducer program that counts the number of tallies for each word and
    write the total number of word occurrences to another text file.

Assumptions:
    The file is some sort of text file. Assume that the provided methods
    work properly. Running on the CSE-Labs machines.

Team members:
    Timothy Kotten : kotte013
    Brett Bodnar : bodna019
    Mouhari Mouhamed : mouha003

Contribrution:
    Equally worked through code together through video call.