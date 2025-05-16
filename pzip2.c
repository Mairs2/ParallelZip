#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <pthread.h>
#include<semaphore.h>
#include <fcntl.h> // open
#include <sys/mman.h> // needed for mmap
#include <sys/sysinfo.h> // needed for get_nprocs()
#include <sys/stat.h> //needed for stat structure and fstat
#include <unistd.h> // sysconf



//STEP 1:
    // represent each compressed segment as a struct
    typedef struct{
        /* data */
        int count;
        char ch;
    }RLEPair;

// STEP 2: Queue that reads each file in chunks for producer-consumer
    #define CHUNCK_s 65536 //chunk size 64KB 
    #define QUEUE_size 16 // good enough buffer to balance 64KB
    #define pairs_per_t 100000// allocates enough mem for compressed thread output to avouid constant malloc

    typedef struct{
        char *data[QUEUE_size];
        size_t size[QUEUE_size];
        int head, tail, count;
        int fin;
        pthread_mutex_t lock;
        pthread_cond_t empthy, full;
    } chunk_Q;  // struct used for the queue that hold the chunk of data read
    chunk_Q queue; // global queue that thread will share when compressing


// STEP 3: create producer that will MMap multiple files 
    
    typedef struct{ // create a list of files for producer to process
        char **names; // filenames for producer to break into chunk
        int count2; // count the number of producer that has been mmapped
    }fileList;

    void *producer(void *arg){
        fileList *files = (fileList *)arg;
        for (int i =0; i< files->count2; i++){ // the checks if file is less than count then mmap's the files
            int fd = open(files->names[i], O_RDONLY, S_IRUSR | S_IWUSR); // opens the files on read only with flags
            struct stat sb; // used for fstat to find file size 
            fstat(fd, &sb); // finds file size for the opened file using the stat buffer
            if (fstat(fd, &sb) == -1 ){ // checks to ensure that we get file size
                printf("Cannot get file size\n");
                return (void *)-1;
            }
            char *fileStart = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE,MAP_PRIVATE, fd, 0); // reads the whole file
            size_t offset = 0;
            while (offset < sb.st_size){
                size_t len = CHUNCK_s; 
                if (offset + len > sb.st_size){
                    len = sb.st_size - offset;
                }
                char *chunk = malloc(len);
                memcpy(chunk, fileStart + offset, len);
                offset += len;
                
                pthread_mutex_lock(&queue.lock);
                while(queue.count == QUEUE_size){// while the queue is full
                    pthread_cond_wait(&queue.empthy, &queue.lock); // tell the consumer to wait becue queue is empthy and locked
                }
                queue.data[queue.tail] = chunk;
                queue.size[queue.tail] = len;
                queue.tail = (queue.tail + 1) % QUEUE_size;
                queue.count++;

                pthread_cond_signal(&queue.full); // signal to producer that queue is full
                pthread_mutex_unlock(&queue.lock);
            }

            munmap(fileStart, sb.st_size); //free mmap after mem allcation for files
            close(fd); //close file descriptor

        }
        pthread_mutex_lock(&queue.lock); // initiate a lock to update the fin variable to 1
        queue.fin = 1; // update flag to show producer is done
        pthread_cond_broadcast(&queue.full); // 
        pthread_mutex_unlock(&queue.lock);
        return NULL;

    }
// STEP 4: create a comsumer that will do the compression of the char in the chunks

    typedef struct{
        RLEPair *pairs;
        size_t count;
    } Tresult; // struct for the result from the threads by consumer
    
    void *consumer(void *arg){//chunks are dequeued, compressed, and reults are stored in a buffer
       Tresult *result = (Tresult *)arg;
       result->pairs = malloc(sizeof (RLEPair) *pairs_per_t );
       result->count = 0;
       
       while (1){
        pthread_mutex_lock(&queue.lock); 
        while (queue.count == 0 && !queue.fin){ // while the queue is empthy and the the producer is not done
            pthread_cond_wait(&queue.full, &queue.lock); // spin till the queue is full and the lock is free
        }
        
        if (queue.count ==0 && queue.fin ){ // checks if the queue is empthy and the producer has finished, there is no more work
            // for the consumer, so it can realease the lock and exit the while loop
            pthread_mutex_unlock(&queue.lock);
            break;
        }

        // we carry on if there is more work from the producer

        char *chunk = queue.data[queue.head]; // the chunk will store the data in the queue starting from the head
        size_t len = queue.size[queue.head]; // len will store the size of the data at the head of the queue
        queue.head = (queue.head +1) % QUEUE_size; // updates the index of where the next chunk witll be dequeued and checks for the 
        //circular queue logic
        queue.count--;  // begins to decrease the count of chunks in the queue as the data is dequeued

        pthread_cond_signal(&queue.empthy); // signals to the producer when the queue is empthy
        pthread_mutex_unlock(&queue.lock); //the lock is released;

        //STEP 3.5: doing the compression using the RLE logic similar to reular zip

        for (size_t i = 0; i< len;){ // for each i less than the size of each chunk in queue
            char ch = chunk[i]; // the charaters in the chunk get stored in at index i get stored in ch
            int cnt = 1; // the char get a count of 1 to start off
            while(i + cnt < len && chunk[i + cnt]== ch){
                // while still inn the bounds of len of chunck, and the nxt char in the chunck is the 
                // same as the char stored in ch, increase the count of that char
                cnt++;
            }
            RLEPair p = {cnt, ch}; // store the count of the char and the char in an array
            result->pairs[result->count++] = p; // store the value of p, in the reault for ouput
            i += cnt; // update the value of i for the next index

        }
        free(chunk); //free the chunk after consumer finishes processing it to avoid mem leak

       }
       return NULL; 

    }

//STEP 5: mearging the chunk outputs from consumer and writing the final output

void merge_output(Tresult *reults, int n_threads){
    if (n_threads == 0 || reults[0].count == 0) return;

    RLEPair prev = reults[0].pairs[0];
    size_t i = 1;

    for(int t = 0; t < n_threads; t++){
        for(; i < reults[t].count; i++){
            RLEPair curr = reults[t].pairs[i];
            if(curr.ch == prev.ch){
                // if the curr char is the same as the prev char, curr count to prev count
                prev.count +=curr.ch;
            } else{
                fwrite(&prev.count, sizeof(int), 1, stdout);
                fwrite(&prev.ch, sizeof(char), 1, stdout);
                prev = curr; // set prev to the curr so that it can be priniteed

            }
        }
        i = 0;
    }
    fwrite(&prev.count, sizeof(int), 1, stdout);
    fwrite(&prev.ch, sizeof(char), 1, stdout);
}


int main(int argc, char *argv[]){
    if (argc < 2){ // checks to make sure there is more than 1 char
        fprintf(stderr, "usage: %s file1 [file2 ...]\n", argv[0]);
        exit(1);
    }

    // initialize the queue mutex and cond variables

    pthread_mutex_init(&queue.lock, NULL);
    pthread_cond_init(&queue.full, NULL);
    pthread_cond_init(&queue.empthy, NULL);

    fileList list_of_file = {.names = &argv[1], .count2 = argc -1};

    // find the number of threads that can be created based on cpu 

    int n_threads = get_nprocs(); // find how many thredas to amke based on processors
    pthread_t prod; //create producer thread
    pthread_t consume[n_threads]; //the size of the conumer threads is determinded by n_thread value
    Tresult results[n_threads] ;// number of reults is determined by the amount of thredas that will gie rsult

    pthread_create(&prod, NULL, producer, &list_of_file); // create thread that uses the producer to chunk the list file
    for(int i = 0; i < n_threads; i++){ // for each thread, use conumer to compress chunks and store it in result
        pthread_create(&consume[i], NULL, consumer, &results[i]);
    }

    pthread_join(prod, NULL);
    for(int i = 0; i < n_threads; i++){ // for each thread, use conumer to compress chunks and store it in result
        pthread_join(consume[i], NULL);
    }

    merge_output(results, n_threads); // merge the results of each thread
    for (int i =0; i < n_threads; i++){
        free(results[i].pairs);
    }

    return 0;
    
}



    