import numpy as np
import hashlib

def assign_group(hotel_id,salt,weights):  
    """generate experiment groups for a hotel id given salt and experiment weights 
        
        Args:
            hotel_id: integer
            salt    : string
            weights : list of integer
            
        Returns:
            integer       
    """

    mhex = hashlib.md5()
    mhex.update(str(hotel_id)+salt)
    splits = np.cumsum(weights)
    nsplits = splits[-1]
    rand = int(mhex.hexdigest()[28:32], 16)%nsplits
    group = np.min(np.argwhere(rand < splits))
    return int(group)
