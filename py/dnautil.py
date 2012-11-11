def _complement(l):
    if l.upper() == "A":
        return "T"
    if l.upper() == "T":
        return "A"
    if l.upper() == "C":
        return "G"
    if l.upper() == "G":
        return "C"

def complement(seq):
    """Return the reverse complement of the sequence"""
    return "".join([_complement(x) for x in seq[::-1]])
