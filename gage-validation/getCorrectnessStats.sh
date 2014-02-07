#!/bin/sh
#
# For more info on mummer see http://mummer.sourceforge.net/manual/
# 
MACHINE=`uname`
PROC=`uname -p`
SCRIPT_PATH=$0
SCRIPT_PATH=`dirname $SCRIPT_PATH`
JAVA_PATH=$SCRIPT_PATH:.

MUMMER=$SCRIPT_PATH
if [ ! -e $MUMMER/nucmer ]; then
   MUMMER=`which nucmer`
   MUMMER=`dirname $MUMMER`
fi

REF=$1
CONTIGS=$2
SCAFFOLDS=$3
MINLENGTH=$4

# CONTIG_FILE is the prefix used for the mummer about about the contigs.
CONTIG_FILE=$(basename $CONTIGS)

# SCAFFOLD_FILE is the prefix used for the mummer about about the scaffolds.
SCAFFOLD_FILE=$(basename $SCAFFOLDS)

echo "Reference:" $REF
echo "Contigs:" $CONTIGS
echo "Scaffolds:" $SCAFFOLDS

# Compute the size of the reference genome.
GENOMESIZE=`java -cp $JAVA_PATH SizeFasta $REF |awk '{SUM+=$NF; print SUM}'|tail -n 1`
echo "Genome size: $GENOMESIZE"

#*****************************************************************************************************************
# Contig stats
#
# Compute statistics about the contigs produced by scaffolding. The contigs are produced by Amos. I think
# they are the result of splitting the scaffolds at gaps.
echo "Contig Stats"

# Compute basic stats about the contigs e.g. total length, num contigs, N50 etc...
java -cp $JAVA_PATH GetFastaStats -o -min ${MINLENGTH} -genomeSize $GENOMESIZE $CONTIGS 2>/dev/null

# nucmer generates nucleotide alignments between two mutli-FASTA input
# files. The out.delta output file lists the distance between insertions
# and deletions that produce maximal scoring alignments between each
# sequence. 
#
# The -p option sets the prefix for the output.
# nucmer produces the file $CONFIG_FILE.delta.
# -l minimum length of a match. The value of 50 was set based on our assembly for human chromosome14.
#   a value of 30 was used for other assemblies
#   TODO(jlewi): We should make this a parameter
# -c minimum length for a set of clusters. The value of 100 was chosen for chromosome 14.  
$MUMMER/nucmer --maxmatch -p $CONTIG_FILE -l 50 -c 100 -banded -D 5 $REF $CONTIGS

# delta-filter filters the alignments produced by nucmer.
$MUMMER/delta-filter -o 95 -i 95 $CONTIG_FILE.delta > $CONTIG_FILE.fdelta

$MUMMER/dnadiff -d $CONTIG_FILE.fdelta

$SCRIPT_PATH/getMummerStats.sh $CONTIGS $SCRIPT_PATH

#*****************************************************************************************************************
# Corrected stats
#
# To get the corrected stats we look at all the alignments of the contigs to the reference.
# For each alignment we look at the length of the reference sequence that matched. We consider
# the matched reference sequence to be the corrected sequence. Thus, we map the contigs to corrected sequence
# and then compute the stats of the corrected sequences.

# out.1coords should be produced by dnadiff using show-coords(http://mummer.sourceforge.net/manual/#coords)
# out.1coords prints summary information about each alignment.
# The columns are [S1] [E1] [S2] [E2] [LEN 1] [LEN 2] [% IDY] [LEN R] [LEN Q] [COV R] [COV Q] [TAGS]
# For a description of the columns see (http://mummer.sourceforge.net/manual/#coords).
# [LEN 1] is the length of alignment region in the reference sequence.
cat out.1coords |awk '{print NR" "$5}' > $CONTIG_FILE.matches.lens

echo ""
echo "Corrected Contig Stats"
# Compute the stats of the lengths of the alignments in the reference genome (these are taken to be the corrected
# contigs).
java -cp $JAVA_PATH:. GetFastaStats -o -min ${MINLENGTH} -genomeSize $GENOMESIZE $CONTIG_FILE.matches.lens 2> /dev/null

#*************************************************************************************************************
# Scaffold data
echo ""
echo "Scaffold Data"

java -cp $JAVA_PATH SplitFastaByLetter $SCAFFOLDS N > tmp_scf.fasta
$MUMMER/nucmer --maxmatch -p $SCAFFOLD_FILE -l 30 -banded -D 5 $REF tmp_scf.fasta
$MUMMER/delta-filter -o 95 -i 95 $SCAFFOLD_FILE.delta > $SCAFFOLD_FILE.fdelta
$MUMMER/show-coords -lrcT $SCAFFOLD_FILE.fdelta | sort -k13 -k1n -k2n > $SCAFFOLD_FILE.coords
$MUMMER/show-tiling -c -l 1 -i 0 -V 0 $SCAFFOLD_FILE.fdelta > $SCAFFOLD_FILE.tiling

echo ""
echo "Scaffold Stats"
java -cp $JAVA_PATH GetFastaStats -o -min 200 -genomeSize $GENOMESIZE $SCAFFOLDS 2> /dev/null
echo "Corrected Scaffold Stats"
java -cp $JAVA_PATH getScaffoldStats $SCAFFOLDS $SCAFFOLD_FILE.tiling $GENOMESIZE $SCAFFOLD_FILE.coords 2> $SCAFFOLD_FILE.err
