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

#include "cm.h"
#include <iostream>
#include <assert.h>
#include <atomic>
#include <cstring>
#include <memory.h>
using namespace std;
          
NBHashMap::NBHashMap()
{
	_old = nullptr;
	_new = nullptr;
	init(16,1.0);
}
NBHashMap::NBHashMap(int capacity,float fac)
{ 		
	_old = nullptr;
	_new = nullptr;
	init(capacity,fac);
}

NBHashMap::~NBHashMap()
{
	if(_old != nullptr && _new != nullptr )
	{
		if( _old != _new)
		{
			delete []_new;
	    }
		
		delete[] _old;
		_new = nullptr;
		_old = nullptr;
	}
}

bool NBHashMap::put(char *k , char *v)
{ //if k exists,return fasle,else put (k,v) in to old_map
	assert(k != nullptr);
	assert(v != nullptr);
	assert(_old != nullptr);
	assert(_new != nullptr);
	
	_entry *oldtmp = _old;
	if(_new == oldtmp) // not resizing
	{
	 // critical path, needs quick operations
	 //compute if it needs resize
    	if(_count >= (int)((double)_maxsize* 0.75))  //  
		{
		// do resize
		cout<< "do resize " << endl;
			int new_maxsize = (int) ((float)_maxsize * (1.0 + _factor));
			
			_entry *newtmp = new _entry[new_maxsize];
			//memset(newtmp,0,sizeof(struct _entry) * new_maxsize); // initialize newtmp
			//_entry *newtmp;
			//newtmp = calloc(new_maxsize, sizeof(_entry));
						
		//cout << "check is it other thread has done resize" << endl;	
			// check if there are other threads trying to resize 
			
			if (_new == oldtmp && _new.compare_exchange_weak(oldtmp, newtmp))			
			{
				// race condition?			
				newtmp = nullptr;	
				_new_maxsize.store(new_maxsize,std::memory_order_relaxed);	
				    cout <<" not resize by other thread,change new size, _newsize =  "<< _new_maxsize << endl; 
				_new_count.store(0,std::memory_order_relaxed);
					cout << " put (" << k<< "," << v << ") to _new" << endl;
				help_put_to_new(k,v);//put (k,v) in to _new
					cout << " copy from _old to _new" << endl; 		  
				copy(); //copy entries from _old to _new
				
			} else {
			cout <<" resize by other thread,directly put to _new" << endl;
				help_put_to_new(k, v);
				
				delete[] newtmp; //not use newtmp
				newtmp = nullptr;
			}			
		}	
		else // no need to do resize
		{
		cout << "no need to resize, put (" << k << "," << v << ")  to _old"<< endl; 
			help_put_to_old(k,v); 
		}	
	}
	else//_new != old: resizing,put (k,v) into _new
	{
	cout << " resizing,put (" << k << "," << v << ")  to _new"<< endl; 
		help_put_to_new(k,v); // put (k,v) to _new
	}
}

char* NBHashMap::get(char *k)
{		
    assert(k != nullptr);
	
	int c = 0,loca = -1;
	if(_new == _old)  /** not resizing,directly check the _old*/
	{
		return help_get_from_old(k,loca);	//loca store the location of k in _old
	}
		
	while(_new != _old) /**resizeing: loop find in first check _new,then _oldmap,until resizing finished*/	
	{
		c++;
		char *v = help_get_from_new(k); //v only can be the value of k or nullptr.
		if(v != nullptr || c > 1)
			return v;
		else  
		{
		//	int i = 0;
			v = help_get_from_old(k,loca); //v only can be a value or COPY_DONE or nullptr
			if(v != COPY_DONE) //exists k in _old,but has been copied to _new,then check _new agin
				continue;
			else if(v == nullptr)// also not exsists in _old
				return v;
			else  // v is the value of k
			{
				help_copy(loca,k,v); // copy the pair to _new and then check _new				
				continue;
			}
		}
	}		
}

bool NBHashMap::remove(char *k)  //////not complete done////////////
{	
    assert(k != nullptr);
	int loca =-1, c = 0;
	if(_new == _old) // _new == _old:not resizing,check _old  		
		return help_remove_from_old(k,loca);
   		
	while(_new != _old)  //resizing: check _old:  
	{    //if not find k:remove from _new
         //       find k: help copy to _new, and the remove from _new
		if(help_remove_from_new(k)) // _new has k,remove it,then change the k in _old to COPY_DONE
		{
			set_copydone(k);
			return true;
		}
		else //_new don't has k,check _old
		{
		    t_old_cnt.fetch_add(1,std::memory_order_relaxed); //add 1 thread opt _old
			for(int i = bkdrHash(k); ; i++)
			{
       			i %= _maxsize;
       			char *probk = _old[i].key.load(std::memory_order_relaxed);       		
    			if(probk != nullptr && string(probk) == string(k))
    			{
    				char *probv = _old[i].value.load(std::memory_order_relaxed);
    				if(probv != COPY_DONE)	
    				{	
    					help_copy(i,k,probv);   		  	 	
    				}
    				t_old_cnt.fetch_sub(1,std::memory_order_relaxed); //sub 1 thread opt _old
    				return help_remove_from_new(k);
				}
				else if(probk == nullptr)
				{
					t_old_cnt.fetch_sub(1,std::memory_order_relaxed); //sub 1 thread opt _old
					return false;
				}
				else
					continue;
			}			
   	   }      
	}
}

void NBHashMap::clear()
{
	_count.store(0,std::memory_order_relaxed);
	_new_count.store(0,std::memory_order_relaxed);
}

void NBHashMap::init(int capacity,float fac)
{
	_maxsize.store(capacity,std::memory_order_relaxed);
	_factor = fac;
	if(_old != nullptr) delete []_old;
	_old = new _entry[_maxsize];	
	//memset(_old,0,sizeof(struct _entry) * _maxsize); // initialize _old
	//_old = calloc(_maxsize,sizeof(_entry));
    t_old_cnt.store(0,std::memory_order_relaxed); //no thread opt _old
	
	if(_old != nullptr){
		_count.store(0,std::memory_order_relaxed);
		_copy_count.store(0,std::memory_order_relaxed);

	
		_new_maxsize.store(capacity,std::memory_order_relaxed);
		_new_count.store(0,std::memory_order_relaxed);
		_entry *oldtmp = _old;
		_new.store(oldtmp,std::memory_order_relaxed);
		oldtmp = nullptr; //
	}
}

void NBHashMap::do_copy(int old_loca,char *k,char *v) //copy (k,v) from _old to _new
{	
    assert(k != nullptr);
    assert(v != nullptr);
    assert(_old != nullptr);
	assert(_new != nullptr);
	
	t_old_cnt.fetch_add(1,std::memory_order_relaxed); //add 1 thread opt _old
	
	_entry *oldtmp = _old;
    int tomb_index = -1; //tomb index in _new;
	for(int i = bkdrHash(k);;i++)
	{
		//char *probk = _old[old_loca].key.load(std::memory_order_relaxed);
		char *oldv = _old[old_loca].value.load(std::memory_order_relaxed);
		//assert(!(probk == nullptr) && !(probk == TOMBSTONE));
		assert(oldv != nullptr && v != COPY_DONE );
		
		if(oldv != COPY_DONE)
		{
			i %= _new_maxsize;
			char *probk = _new[i].key.load(std::memory_order_relaxed);
			if(probk !=nullptr && string(probk) == string(k)) // probk == k
			{
			// true:a new k has put to _new by other htread, then throw away the old value.
				if(_old[old_loca].value.compare_exchange_weak(v,COPY_DONE))				
				{
					_copy_count.fetch_add(1,std::memory_order_relaxed);
					if(_copy_count == _count)
					{
					
						if(t_old_cnt.compare_exchange_weak(2,1))//if no other thread opt _old,then change _old point to _new
						{
							_entry *newtmp = _new;				
							if(_old.compare_exchange_weak(oldtmp, newtmp)) //_old = _new;
							{
								_maxsize.store(_new_maxsize,std::memory_order_relaxed);
								_count.store(_new_count,std::memory_order_relaxed);
								_copy_count.store(0,std::memory_order_relaxed);		
													
								delete[] oldtmp;					
								oldtmp = nullptr;
								newtmp = nullptr;	
								return;
							}										
						}	                             
					}					
				}				
				break;  // false: (k,v) was copied by other thread.
			}
			else  //probk != k
			{		
				if(probk != nullptr)
				{
					if(probk != TOMBSTONE)
						continue;
					else
					{
						tomb_index = i;
						continue;
					}
				} 
				else		
				{
					if(tomb_index != -1) //put to the tomb_index location
					{	
					
						if(_new[tomb_index].key.compare_exchange_weak(probk,k))
						{
							cout << "_new index: " << i << endl;
							char *prek = _new[tomb_index].key.load(std::memory_order_relaxed);
							char *prev = _new[tomb_index].value.load(std::memory_order_relaxed);
							if( prek != nullptr && string(prek) != string(k) && prek != TOMBSTONE)
								continue;	
									
							//_new[i].value.store(v,std::memory_order_relaxed);						
							if(_new[tomb_index].value.compare_exchange_weak(prev,v))
								_new_count.fetch_add(1,std::memory_order_relaxed);		
			// if faild,say new k,v' has put into _newthen directly set COPY_DONE to _old[local] 
							
							if(_old[old_loca].value.compare_exchange_weak(v,COPY_DONE))
							{
								_copy_count.fetch_add(1,std::memory_order_relaxed);	
								
								if(_copy_count == _count) //test whether finish copy
								{				
					          	 //if no other thread opt _old,then change _old point to _new
									if(t_old_cnt.compare_exchange_weak(2,1))								
									{
										_entry *newtmp = _new;
										if(_old.compare_exchange_weak(oldtmp, newtmp)) //_old = _new;
										{
											_maxsize.store(_new_maxsize,std::memory_order_relaxed);
											_count.store(_new_count,std::memory_order_relaxed);
									
											delete[] oldtmp;					
							        		oldtmp = nullptr;
							        		newtmp = nullptr;
							        		return;
										}
									}
								}
							}            						
							else //COPY_DONE set by other thread
								break; 														
						}
						else  //another thread has put another key to _new[i]
							continue;							
					}
					else //put to the i location
					{
					
						if(_new[i].key.compare_exchange_weak(probk,k))
						{
							cout << "_new index: " << i << endl;
							char *prek = _new[i].key.load(std::memory_order_relaxed);
							char *prev = _new[i].value.load(std::memory_order_relaxed);
							if(prek != nullptr && string(prek) != string(k) && prek != TOMBSTONE)
								continue;								
							if(_new[i].value.compare_exchange_weak(prev,v))
							//_new[i].value.store(v,std::memory_order_relaxed);
								_new_count.fetch_add(1,std::memory_order_relaxed);											
							// if faild,say new k,v' has put into _newthen directly set COPY_DONE to _old[local] 
							
							if(_old[old_loca].value.compare_exchange_weak(v,COPY_DONE))
							{
								_copy_count.fetch_add(1,std::memory_order_relaxed);	
								
								if(_copy_count == _count) //test finish copy
								{			
									if(t_old_cnt.compare_exchange_weak(2,1))								
									{
										_entry *newtmp = _new;
										if(_old.compare_exchange_weak(oldtmp, newtmp)) //_old = _new;
										{
											_maxsize.store(_new_maxsize,std::memory_order_relaxed);
											_count.store(_new_count,std::memory_order_relaxed);
									
											delete[] oldtmp;					
							        		oldtmp = nullptr;
							        		newtmp = nullptr;
							        		return;
							       		}
									}
								}
							}	                   	
							else  //COPY_DONE set by other thread
								break;						
						}
						else  //another thread has put a another key to _new[i]
							continue;		
					}
				}										
			}					
		}
	    break;
	}
	t_old_cnt.fetch_sub(1,std::memory_order_relaxed); //sub 1 thread opt _old		
	return;
}

void NBHashMap::copy() // copy all the pairs from _old to _new
{
//	new_maxsize.store((int) ((float)maxsize * (1.0 + fator)),std::memory_order_relaxed);	
//	_new = new _entry[new_maxsize];
//	new_count.store(0,std::memory_order_relaxed);
	t_old_cnt.fetch_add(1,std::memory_order_relaxed); //add 1 thread opt _old		
	for(int i = 0; i < _maxsize; i++)
	{        		
        char *probk = _old[i].key.load(std::memory_order_relaxed);     
        if(probk != nullptr)
        {
        	char *probv = _old[i].value.load(std::memory_order_relaxed);
        	if(probk != TOMBSTONE && probv != COPY_DONE && probv != nullptr)     
        	{
        	cout << "copy the " << i << " one: (" << probk << ","<<probv <<")" <<endl;
        	
        		do_copy(i,probk,probv);   
        	}    	
        }      
	}
    				
} 

void NBHashMap::help_copy(int old_loca,char *k,char *v)
{
    assert(k != nullptr);
	do_copy(old_loca,k,v); 
}

bool NBHashMap::set_copydone(char *k)
{
    assert(k != nullptr);
    assert(_old != nullptr);

	for(int i = bkdrHash(k); ; i++)
	{
    	i %= _maxsize;
       	char *probk = _old[i].key.load(std::memory_order_relaxed);
       	if(probk == nullptr)
       		return false;
    	else if(string(probk) == string(k))
    	{
    		char *probv = _old[i].value.load(std::memory_order_relaxed);
    		if(probv != COPY_DONE)	
    		{
    			if(_old[i].value.compare_exchange_weak(probv,COPY_DONE))
    			{
    				_copy_count.fetch_add(1,std::memory_order_relaxed);
    				return true;
    			}
    		}
    		return false;		
    	}
    	else
    		continue;
	}
}

bool NBHashMap::help_put_to_new(char *k, char *v) //put (k,v) in to _new
{
    assert(k != nullptr);
	assert(_new != nullptr);
	
	int tomb_index = -1;
	for(int i = bkdrHash(k); ; i++ )
	{
		i %= _new_maxsize;
		char *probk =  _new[i].key.load(std::memory_order_relaxed);
		char *probv = _new[i].value.load(std::memory_order_relaxed);
		if(probk != nullptr && string(probk) == string(k))					
		{
			_new[i].value.compare_exchange_weak(probv,v);
		//_new_count.fetch_add(1,memory_order_relaxed);			
			return true;		
		}	
		else
		{	
			if(probk != nullptr)
			{
				if(probk != TOMBSTONE)
					continue;
				else
				{
					tomb_index = i;
					continue;
				}
			} 
			else		
			{
				if(tomb_index != -1) // the location : tomb_index in _new is available
				{
					_new[tomb_index].key.compare_exchange_weak(probk,k);
					char *prek = _new[tomb_index].key.load(std::memory_order_relaxed);
					if( prek != nullptr && string(prek) != string(k) && prek != TOMBSTONE)
						continue;	
					else
					{
					cout << "_new index: " << tomb_index << endl;
						_new[tomb_index].value.compare_exchange_weak(probv,v);
						_new_count.fetch_add(1,memory_order_relaxed);	
						return true;
					}								
				}	
				else // the location : i in _new is available
				{
				
					_new[i].key.compare_exchange_weak(probk,k);
					char *prek = _new[i].key.load(std::memory_order_relaxed);
					if( prek != nullptr && string(prek) != string(k) && prek != TOMBSTONE)
						continue;	
					else
					{
					cout << "_new index: " << i << endl;
						_new[i].value.compare_exchange_weak(probv,v);
						_new_count.fetch_add(1,memory_order_relaxed);	
						return true;
					}			
				}
			}						
		}		
		
	}	
}

bool NBHashMap::help_put_to_old(char *k,char *v ) //put (k,v) in to _old
{
    assert(k != nullptr);
    assert(v != nullptr);
    assert(_old != nullptr);
	
	int tomb_index = -1;
	for(int i = bkdrHash(k); ; i++ )
	{
		i %= _maxsize;
		char *probk =  _old[i].key.load(std::memory_order_relaxed);
		char *probv =  _old[i].value.load(std::memory_order_relaxed);
		if(probk != nullptr && string(probk) == string(k))					
		{
		cout << k << " exists "<< endl;
			if(_old[i].value.compare_exchange_weak(probv,v))
				cout << k << " exists, change v to " << v << endl;
		//_new_count.fetch_add(1,memory_order_relaxed);			
			return true;		
		}	
		else
		{
			cout << k << " not exists"<< endl;
			if(probk != nullptr)
			{
				if(probk != TOMBSTONE)
					continue;
				else
				{
					if(tomb_index == -1)
						tomb_index = i;
					continue;
				}
			} 
			else		
			{
				if(tomb_index != -1) // the location : tomb_index in _old is available
				{
					_old[tomb_index].key.compare_exchange_weak(probk,k);
					char *prek = _old[tomb_index].key.load(std::memory_order_relaxed);
					char *prev =  _old[tomb_index].value.load(std::memory_order_relaxed);
					if(prek != nullptr && string(prek) != string(k) && prek != TOMBSTONE)
					{
						//i = tomb_index ; //whether continue begin tomb_index
						continue;	
					}
					else
					{
					cout << "_old index: " << tomb_index << endl;
						_old[tomb_index].value.compare_exchange_weak(prev,v);
						_count.fetch_add(1,memory_order_relaxed);	
						return true;
					}								
				}	
				else  //the location : i in _old is available
				{
					_old[i].key.compare_exchange_weak(probk,k);
					char *prek = _old[i].key.load(std::memory_order_relaxed);
					if(prek != k && prek != nullptr && prek != TOMBSTONE)
						continue;	
					else
					{
					cout << "_old_index " << i << endl;
						_old[i].value.compare_exchange_weak(probv,v);
						_count.fetch_add(1,memory_order_relaxed);	
						return true;
					}			
				}
			}									
		}
		
	}	
}

char* NBHashMap::help_get_from_old(char *k,int &loca) //loca is the location of the (k,v)
{
    assert(k != nullptr);
    assert(_old != nullptr);
	
	int i = bkdrHash(k);
	int hash = i;
	for(; ; i++)
	{
		i %= _maxsize;
		loca = i;
		char *probk =_old[i].key.load(std::memory_order_relaxed);			
		if(probk != nullptr && string(probk)== string(k))		
			return _old[i].value.load(std::memory_order_relaxed);
		if(probk == nullptr) 
			return nullptr;	
	}	
}

char* NBHashMap::help_get_from_new(char *k) //loca is the location of the (k,v)
{
    assert(k != nullptr);
	assert(_new != nullptr);
	
	for(int i = bkdrHash(k); ; i++)
	{
		i %= _new_maxsize;
		char *probk =_new[i].key.load(std::memory_order_relaxed);			
		if(probk != nullptr && string(probk)== string(k))		
			return _new[i].value.load(std::memory_order_relaxed);
		if(probk == nullptr) 
			return nullptr;
	}	
}

bool  NBHashMap::help_remove_from_old(char *k,int loca) //loca is the location of k in _old
{	
    assert(k != nullptr);
    assert(_old != nullptr);

	for(int i = bkdrHash(k);;i++)
	{
		i %= _maxsize;
		loca = i;
		char *delkey = _old[i].key.load(std::memory_order_relaxed);	
		if(delkey == nullptr)
			return false;
		if(delkey != nullptr && string(delkey) == string(k))		
		{
			if(_old[i].key.compare_exchange_weak(k,TOMBSTONE));
			{
				_count.fetch_sub(1,memory_order_relaxed);
			}
			return true; // or remove by other thread
		}			
	}
}

//bool help_cpoy_and_remove(i,k,probv)

bool  NBHashMap::help_remove_from_new(char *k) 
{
    assert(k != nullptr);
	assert(_new != nullptr);
	
	for(int i = bkdrHash(k);;i++)
	{
		i %= _maxsize;		
		char *delkey = _new[i].key.load(std::memory_order_relaxed);	
		if(delkey == nullptr)
			return false;
		if(delkey != nullptr && string(delkey) == string(k))		
		{
			if(_new[i].key.compare_exchange_weak(k,TOMBSTONE));
			{
				_new_count.fetch_sub(1,memory_order_relaxed);
			}
			return true; //or removed other thread
		}			
	}
}

int NBHashMap::bkdrHash(char *str) // not complete done
{
	unsigned int seed = 31;
    unsigned int hash = 0;
 /**
    while(*str)
    {
        hash += hash * seed + (*str++);
        
    }
    return (hash & 0x7FFFFFFF);
    */
    return 0;
}

/**
unsigned int adjustSize(unsigned int nsize)
{
	 int i = 0;
	for( ; i < 28; i++)
	{
		if(size_Array[i] > nsize)
			break;
	}
	if(i == 28)
		i--;
	return size_Array[i];
}


int NBHashMap::find(_entry *_map,int size,char *k)  //return the location of k or an available location in _map
{
	for(int i =  bkdrHash(k); ; i++)
	{
		i %= size;
		char *probk = _map[i].key.load(std::memory_order_relaxed);
		if(probk == k || probk == nullptr || probk == TOMBSTONE)
			return i;
	}
}
*/

