import imagehash
import numpy as np


def compute_image_hash(img):
    """Compute a combined perceptual hash for an image.
    
    Currently, this is just the concatenation of the dHash and the avgHash.
    
    Args:
        img (PIL.Image): An Image to hash.

    Returns:
        A `uint8` ndarray.
    """

    h1 = imagehash.dhash(img)
    h1 = np.packbits(np.where(h1.hash.flatten(), 1, 0))

    h2 = imagehash.average_hash(img)
    h2 = np.packbits(np.where(h2.hash.flatten(), 1, 0))

    return np.concatenate((h1, h2))


def hamming_dist(h1, h2):
    """Compute the Hamming distance between two uint8 arrays.
    """

    return np.count_nonzero(np.unpackbits(np.bitwise_xor(h1, h2)))


def construct_hash_idx_key(idx, val):
    return "hash_idx:{:02d}:{:02x}".format(idx, val).encode("utf-8")


def exists_in_index(redis, imhash, min_threshold=64):
    """Check if any image exists in the image with a nearby hash.
    
    Args:
        redis (redis.Redis): A Redis interface.
        imhash (ndarray): An image hash to look up. Must be of type `uint8`.
        min_threshold (int): A minimum distance threshold for filtering results.
            
    Returns:
        bool: True if a closely-matching image exists, False otherwise.
    """

    h_bytes = imhash.tobytes()

    keys = []
    for idx, val in enumerate(h_bytes):
        keys.append(construct_hash_idx_key(idx, val))

    hashes = redis.sunion(*keys)
    _t = []

    for h in hashes:
        arr = np.frombuffer(h, dtype=np.uint8)
        dist = hamming_dist(arr, imhash)

        if dist < min_threshold:
            return True

    return False


def search_index(redis, imhash, min_threshold=64):
    """Search the index for images with nearby hashes.
    
    Args:
        redis (redis.Redis): A Redis interface.
        imhash (ndarray): An image hash to look up. Must be of type `uint8`.
        min_threshold (int): A minimum distance threshold for filtering results.
            The result list will only contain images with a result less than
            this value.
            
    Returns:
        A list of (hash, distance) tuples, sorted by increasing distance.
    """

    h_bytes = imhash.tobytes()

    keys = []
    for idx, val in enumerate(h_bytes):
        keys.append(construct_hash_idx_key(idx, val))

    hashes = redis.sunion(*keys)
    _t = []

    for h in hashes:
        arr = np.frombuffer(h, dtype=np.uint8)
        dist = hamming_dist(arr, imhash)

        if dist < min_threshold:
            _t.append((h, dist))

    return sorted(_t, key=lambda o: o[1])
