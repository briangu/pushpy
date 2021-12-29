from multiprocessing import shared_memory
shm_a = shared_memory.SharedMemory(create=True, size=10)
print(type(shm_a.buf))
shm_a.close()
shm_a.unlink()

