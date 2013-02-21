"""A simple script to convert the histogram of kmer frequencies produced
by the cutoff calculation to a csv file.
"""
import sys

def run():
    with file(sys.argv[1], "r") as hf:
        for line in hf:
            pieces = line.split()
            print pieces[0] + "," + pieces[1]

if __name__ == "__main__":
    run()
