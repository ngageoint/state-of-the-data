import numpy as np
def _c(ca,i,j,P,Q):
    """
    Adapted from: http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf

    """
    if ca[i,j] > -1:
        return ca[i,j]
    elif i == 0 and j == 0:
        ca[i,j] = distance(P[0],Q[0])
    elif i > 0 and j == 0:
        ca[i,j] = max(_c(ca,i-1,0,P,Q),distance(P[i],Q[0]))
    elif i == 0 and j > 0:
        ca[i,j] = max(_c(ca,0,j-1,P,Q),distance(P[0],Q[j]))
    elif i > 0 and j > 0:
        ca[i,j] = max(min(_c(ca,i-1,j,P,Q),_c(ca,i-1,j-1,P,Q),_c(ca,i,j-1,P,Q)),distance(P[i],Q[j]))
    else:
        ca[i,j] = float("inf")
    return ca[i,j]


def frechet(P,Q):
    """
    Computes the discrete frechet distance between two polylines or
    polygons

    Usage:
       frechet(P=[[0,0], [20,20]], Q=[[10,10], [5,5]])
    """
    ca = np.ones((len(P),len(Q)))
    ca = np.multiply(ca,-1)
    return _c(ca,len(P)-1,len(Q)-1,P,Q)

