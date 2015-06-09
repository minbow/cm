/* -*- Mode: C; indent-tabs-mode: t; c-basic-offset: 4; tab-width: 4 -*-  */
/*
 * nb-hash-map.h
 * Copyright (C) 2015 min <min@min-Lenovo-Product>
 *
 * foobar-cpp is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * foobar-cpp is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
 
//25/05/2015 

#ifndef _NBHASHMAP_H_
#define _NBHASHMAP_H_
#include <iostream>
#include <atomic>
using namespace std;

const unsigned long size_Array[30] = {
	53,97,193,389,769,
	1543,3097,6151,12289,24593,
	49157,98317,196613,393241,786443,
	1572869,3145739,6291469,12582917,25165842,
	50331553,100663319,201326611,402653189,805306457,
	1610612741,3221225473ul,4294967291ul
	};

struct _entry{
	//data members: atomic type key and value
	std::atomic<char*> key; 
	std::atomic<char*> value;
	
	//constructor
	_entry():key(nullptr),value(nullptr){}
	
	_entry(char *k,char *v)
	{
		key.store(k,std::memory_order_relaxed);
		value.store(v,std::memory_order_relaxed);
	}  
};

class NBHashMap
{	
public:
	NBHashMap();
	NBHashMap(int capacity,float fac);
	~NBHashMap();
	bool put(char *k , char *v);
	char* get(char *k);	
	bool remove(char *k);
  	void clear();
  	
private:
	//data members
	atomic<_entry *> _old;
	atomic<_entry *> _new;
    float _factor;  //resize factor
    std::atomic<int> _maxsize;  //capacity of _old
	std::atomic<int> _count;    //<k,v> pair count of _old
	std::atomic<int> _new_maxsize;  //capacity of _new
	std::atomic<int> _new_count;   //<k,v> pair count of _new
	std::atomic<int> _copy_count;  //<k,v> pair count have copied from _old to _new,if copy done, copy_count equals count
		
	char* TOMBSTONE =(char*)-1;   //mark the k where <k,v> pair is removed
    char* COPY_DONE = (char*)-2;  //mark the value where <k,v> has been copied to _new
    static const int inc = 2;
    static const int step = 1;
    std::atomic<int> t_old_cnt;
    
	//help functions
	void init(int capacity,float fac);
	void do_copy(int old_loca,char *k,char *v); //copy the old_loca member to _new;
	void copy();//copy all pairs from _old to _new;
	void help_copy(int old_loca,char *k,char *v); //other thread help copy the old_loca member to _new;
	bool set_copydone(char *k);
	bool help_put_to_new(char *k, char *v); //put (k,v) in to _new
	bool help_put_to_old(char *k,char *v ); //put (k,v) in to _old
	char* help_get_from_old(char *k,int &loca); //get (k,v) from to _new, loca is the location for k in _old
	char* help_get_from_new(char *k);  //get value from _new where key==value
	bool  help_remove_from_old(char *k,int &loca);  //remove <k,v> in _old, and loca is the location of the k in _old
	bool  help_remove_from_new(char *k);  //remove <k,v> in _new
	int bkdrHash(char *key);  // return the hash number of k in _map	
	unsigned int adjustSize(unsigned int size);
	int find(_entry *_map,int size,char *k); //return a vailable location in _map
};

#endif // _NB_HASH_MAP_H_


