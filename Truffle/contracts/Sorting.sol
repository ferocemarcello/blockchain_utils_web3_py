pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./Platform.sol" as Pl;
import "./Lib.sol" as Lb;

contract Sorting {
    Lb.Lib lib = new Lb.Lib();
    event logString(string arg);
    
    //perform the quicksort over an array, given in input. it doesn't return anything since it modifies the given input inside
    //left and right are mark the start and the end index in between which, the quicksort is performed.
    //it is a recursive algorithm
    function quickSort(uint[] memory arr, int left, int right) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;
	    uint pivot = arr[uint(left + (right - left) / 2)];
	    while (i <= j) {
	        while (arr[uint(i)] < pivot) i++;
	        while (pivot < arr[uint(j)]) j--;
	        if (i <= j) {
	            (arr[uint(i)], arr[uint(j)]) = (arr[uint(j)], arr[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort(arr, left, j);
	    if (i < right)
	        quickSort(arr, i, right);
	}

	//same as quicksort(). Also, it modifies the list of indices of the array.
	//It can be useful if with these new indices, one wants to reorder a second array
	function quickSort_indices(uint[] memory arr, int left, int right, uint[] memory indices) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;

	    uint pivot = arr[uint(left + (right - left) / 2)];

	    while (i <= j) {
            while (arr[uint(i)] < pivot) i++;
	        while (pivot < arr[uint(j)]) j--;
	        if (i <= j) {
	            (arr[uint(i)], arr[uint(j)]) = (arr[uint(j)], arr[uint(i)]);
	            (indices[uint(i)], indices[uint(j)]) = (indices[uint(j)], indices[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort_indices(arr, left, j, indices);
	    if (i < right)
	        quickSort_indices(arr, i, right, indices);
	}
	//same as quickSort_indices(). it performs the quicksort over two arrays. the first array as priority
	function quickSort_indices_two_arr(uint[] memory arr_first, uint[] memory arr_second, int left, int right, uint[] memory indices, bool ascending_first, bool ascending_second) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;

	    int pivot_ind = left + (right - left) / 2;
	    uint pivot = arr_first[uint(pivot_ind)];
	    uint pivot_second = arr_second[uint(pivot_ind)];
	    
	    while (i <= j) {
	        if(ascending_first) {
	            if(ascending_second) {
	                while (arr_first[uint(i)] < pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] < pivot_second)) i++;
	        	    while (pivot < arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] < arr_second[uint(j)])) j--;
	            }
	            else{
	                while (arr_first[uint(i)] < pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] > pivot_second)) i++;
	        	    while (pivot < arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] > arr_second[uint(j)])) j--;
	            }
	        }
	        else {
	            if(ascending_second) {
	                while (arr_first[uint(i)] > pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] < pivot_second)) i++;
	        	    while (pivot > arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] < arr_second[uint(j)])) j--;
	            }
	            else{
	                while (arr_first[uint(i)] > pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] > pivot_second)) i++;
	        	    while (pivot > arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] > arr_second[uint(j)])) j--;
	            }
	        }
	        if (i <= j) {
	            (arr_first[uint(i)], arr_first[uint(j)]) = (arr_first[uint(j)], arr_first[uint(i)]);
	            (arr_second[uint(i)], arr_second[uint(j)]) = (arr_second[uint(j)], arr_second[uint(i)]);
	            (indices[uint(i)], indices[uint(j)]) = (indices[uint(j)], indices[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort_indices_two_arr(arr_first, arr_second, left, j, indices, ascending_first, ascending_second);
	    if (i < right)
	        quickSort_indices_two_arr(arr_first, arr_second, i, right, indices, ascending_first, ascending_second);
	}
	//sort an array using the countingsort. it returns the sorted array
	function countingSort(uint[] memory data, bool ascending) public view returns(uint[] memory){
	    uint max = lib.maxArray(data);
	    uint min = lib.minArray(data);
	    uint[] memory sorted = new uint[](data.length);
	    uint[] memory count = new uint[](max-min);
	    for (uint i = 0; i < data.length; i++) {
	        data[i]=data[i]-min;
	        count[data[i]]++;
        }
        uint j=0;
        if(ascending) {
            for (uint i = 0; i < count.length; i++) {
                while (count[i] > 0) {
                    sorted[j] = count[i];
                    j++;
                }
            }
        }
        else {
            for (uint i = count.length-1; i >=0; i--) {
                while (count[i] > 0) {
                    sorted[j]=count[i];
                    j++;
                }
            }
        }
        
        return sorted;
	}
	//sort an array using the countingsort. It doesn't return since the array is already modified inside
	function countingSort_void(uint[] memory data, uint setSize) public pure {
        uint[] memory set = new uint[](setSize);
        for (uint i = 0; i < data.length; i++) {
            set[data[i]]++;
        }
        uint j = 0;
        for (uint i = 0; i < setSize; i++) {
            while (set[i]-- > 0) {
                data[j] = i;
                if (++j >= data.length) break;
            }
        }
    }
	//same as countingSort() but it returns the indices of the sorted array
	function countingSort_indices(uint[] memory data, bool ascending, uint start, uint end) public view returns(uint[] memory){
	    uint[] memory data_cropped = lib.cropArray(data, start, end);
	    //data_cropped = normalizeArr(data_cropped);
	    
	    uint[] memory indices = lib.getIndices(data.length);
	    
	    uint[] memory sorted_indices = new uint[](indices.length);
	    for (uint i = 0; i < sorted_indices.length; i++) {
            sorted_indices[i] = indices[i];
	    }
	    
	    uint[] memory count = lib.getCount(data_cropped);
	    
        //count size = max-min+1
        //data size = full data.length(no start/end)
        //indices size = full data.length(no start/end)
        uint[][] memory count_indices = lib.getCountIndices(count, data, indices, start, end);
        //count_indices size = max-min+1 data cropped
        
        uint z = start;
        if(ascending) {
            for(uint i = 0; i < count_indices.length; i++){
                for(uint j = 0; j < count_indices[i].length; j++) {
                    sorted_indices[z] = count_indices[i][j];
                    z++;
                }
            }
        }
        else {
            for(int i = int(count_indices.length-1); i >= 0; i--){
                for(uint j = 0; j < count_indices[uint(i)].length; j++) {
                    sorted_indices[z] = count_indices[uint(i)][j];
                    z++;
                }
            }
        }
        
        return sorted_indices;
	}
	//using countingsort, it sort an array of integers, then it sorts an array of OfferBid based on the same sorting and returns it.
	function get_indices_and_sort_countingsort(uint[] memory values, Lb.Lib.OfferBid[] memory offers_bids, bool ascending) private view returns(Lb.Lib.OfferBid[] memory) {
		uint[] memory sorted_indices = Sorting.countingSort_indices(values, ascending, 0, values.length-1);
		Lb.Lib.OfferBid[] memory sorted = new Lb.Lib.OfferBid[](values.length);
		for (uint i = 0; i < sorted_indices.length; i++) {
            sorted[i] = offers_bids[sorted_indices[i]];
	    }
	    return sorted;
	}
	//same as get_indices_and_sort_countingsort(), but it sorts using two arrays
	function get_indices_and_sort_countingsort_two_arr(uint[] memory values_first, uint[] memory values_second, Lb.Lib.OfferBid[] memory offers_bids, bool ascending_first, bool ascending_second) public view returns(Lb.Lib.OfferBid[] memory) {
		uint[] memory sorted_indices = Sorting.countingSort_indices(values_first, ascending_first, 0, values_first.length-1);
		
		uint[] memory sorted_first = lib.reorderArr(sorted_indices, values_first, 0, values_first.length-1);
		uint[] memory reordered_second = lib.reorderArr(sorted_indices, values_second, 0, values_second.length-1);
		
	    sorted_indices = countingsort_by_second_value_indices(sorted_indices, sorted_first, reordered_second, ascending_second);
	    
	    Lb.Lib.OfferBid[] memory sorted_offers_bids = lib.reorderArr_OfferBid(sorted_indices, offers_bids);

	    return sorted_offers_bids;
	}
	//it reorders an array based on the sorting done on another array. the sorting is done using the countingsort
	function sort_and_reorder_arr(uint[] memory data, bool ascending, uint start, uint end, uint[] memory arr) public view returns(uint[] memory) {
	    uint[] memory modified_indices = countingSort_indices(data, ascending, start, end);
	    uint[] memory reordered_arr = lib.reorderArr(modified_indices, arr, 0, arr.length-1);
	    return reordered_arr;
	}
	//sorts a second array, in case of same value in a first array
	function countingsort_by_second_value_indices(uint[] memory sorted_indices, uint[] memory sorted_first, uint[] memory values_second, bool ascending) public view returns(uint[] memory){
	    uint[] memory indices = new uint[](sorted_indices.length);
	    for (uint i = 0; i < indices.length; i++) {
            indices[i] = sorted_indices[i];
	    }
	    
	    uint i = 0;
	    uint count = 0;
	    while (i < sorted_first.length) {
	        count = lib.find_num_same_value(sorted_first, sorted_first[i], true);
	        if(count > 1) {
	            uint start = i;
	            uint end = i + count - 1;
	            indices = sort_and_reorder_arr(values_second, ascending, start, end, indices);
	        }
	        i = i + count;
	    }
        return indices;
	}
	//using quicksort, it sort an array of integers, then it sorts an array of OfferBid based on the same sorting and returns it.
	function get_indices_and_sort_quicksort(uint[] memory values, Lb.Lib.OfferBid[] memory offers_bids, bool ascending) private view returns(Lb.Lib.OfferBid[] memory) {
	    uint[] memory indices = new uint[](values.length);
	    for (uint z = 0; z < indices.length; z++) {
            indices[z] = z;
	    }
		Sorting.quickSort_indices(values, 0, int(values.length-1), indices);
		if(!ascending){
		    indices = lib.reverseArray(indices, 0, indices.length - 1);
		}
		Lb.Lib.OfferBid[] memory sorted = new Lb.Lib.OfferBid[](values.length);
		for (uint z = 0; z < indices.length; z++) {
            sorted[z] = offers_bids[indices[z]];
	    }
	    return sorted;
	}
	//same as get_indices_and_sort_quicksort(), but it sorts using two arrays
	function get_indices_and_sort_two_arr_quicksort(uint[] memory values_first, uint[] memory values_second, Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price, bool ascending_quantity) private pure returns(Lb.Lib.OfferBid[] memory) {
	    uint[] memory indices = new uint[](values_first.length);
	    for (uint z = 0; z < indices.length; z++) {
	        indices[z] = z;
	    }
		Sorting.quickSort_indices_two_arr(values_first, values_second, 0, int(values_first.length-1), indices, ascending_price, ascending_quantity);
		Lb.Lib.OfferBid[] memory sorted = new Lb.Lib.OfferBid[](values_first.length);

		for (uint z = 0; z < indices.length; z++) {
		    sorted[z] = offers_bids[indices[z]];
		}
	    return sorted;
	}
	//using quicksort, sorts a list of OfferBid by ts_delivery
	function quickSortOffersBidsTsDelivery(Lb.Lib.OfferBid[] memory arr, bool ascending) public view returns(Lb.Lib.OfferBid[] memory) {
		if(arr.length == 0) return arr;
		uint[] memory ts_deliveries = lib.arr_of_ts_deliveries_offerbids(arr);
		Lb.Lib.OfferBid[] memory sorted = get_indices_and_sort_quicksort(ts_deliveries,arr,ascending);
		return sorted;
	}
	//using quicksort, sorts a list of OfferBid by price
	function quickSortOffersBidsPrice(Lb.Lib.OfferBid[] memory offers_bids, bool ascending) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		Lb.Lib.OfferBid[] memory sorted = get_indices_and_sort_quicksort(prices,offers_bids,ascending);
		return sorted;
	}
	//using quicksort, sorts a list of OfferBid by price and then quantity
	function quickSortOffersBidsPrice_Quantity(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price,  bool ascending_quantity) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		Lb.Lib.OfferBid[] memory sorted = get_indices_and_sort_two_arr_quicksort(prices,quantities,offers_bids,ascending_price, ascending_quantity);
		return sorted;
	}
	//using countingsort, sorts a list of OfferBid by price
	function countingSortOffersBidsPrice(Lb.Lib.OfferBid[] memory offers_bids, bool ascending) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		Lb.Lib.OfferBid[] memory sorted = get_indices_and_sort_countingsort(prices,offers_bids,ascending);
		return sorted;
	}
	//using countingsort, sorts a list of OfferBid by price and then quantity
	function countingSortOffersBidsPriceQuantity(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price, bool ascending_quantity) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		Lb.Lib.OfferBid[] memory sorted = get_indices_and_sort_countingsort_two_arr(prices, quantities, offers_bids, ascending_price, ascending_quantity);
		return sorted;
	}
	//gets the indices of the a sorted array, using insertion_sort
	function getInsertionSortIndices(uint[] memory arr_first, uint[] memory arr_second, bool ascending_first, bool ascending_second) public view returns(uint[] memory) {
	    uint[] memory new_indices = new uint[](0);
		bool go_on = true;
		new_indices = new uint[](1);
		new_indices[0] = 0;
		for(uint i = 1; i < arr_first.length; i++) {
	        go_on = true;
	         if (lib.compare(arr_first[i], arr_first[new_indices[0]], arr_second[i], arr_second[new_indices[0]], ascending_first, ascending_second)) {
	            new_indices = lib.add_pos(new_indices, 0);//shift right
	            new_indices[0] = i;
	            go_on = false;
	        }
	        else if(lib.compare(arr_first[new_indices[new_indices.length - 1]], arr_first[i], arr_second[new_indices[new_indices.length - 1]], arr_second[i], ascending_first, ascending_second)) {
	            new_indices = lib.add_pos(new_indices, new_indices.length);//shift left
	            new_indices[new_indices.length-1] = i;
	            go_on = false;
	        }
	        if(go_on && lib.compare(arr_first[new_indices[0]], arr_first[i], arr_second[new_indices[0]], arr_second[i], ascending_first, ascending_second) && lib.compare(arr_first[i], arr_first[new_indices[new_indices.length - 1]], arr_second[i], arr_second[new_indices[new_indices.length - 1]], ascending_first, ascending_second)) {
	            go_on = true;
	            uint z = 0;
	            while(go_on) {
	                if(lib.compare(arr_first[i], arr_first[new_indices[z]], arr_second[i], arr_second[new_indices[z]], ascending_first, ascending_second)) {
	                    new_indices = lib.add_pos(new_indices, z);
	                    new_indices[z] = i;
	                    go_on = false;
	                }
	                z++;
	            }
	        }
		}
		return new_indices;
	}
	//same as getInsertionSortIndices, different version not optimized
	function getInsertionSortIndices_not_optimized(uint[] memory arr_first, uint[] memory arr_second, bool ascending_first, bool ascending_second) public view returns(uint[] memory) {
	    uint[] memory new_indices = lib.getIndices(arr_first.length);
		uint new_ind;
		
		uint[] memory arr_first_new = lib.copyArray(arr_first, 0, 1);
        uint[] memory arr_second_new = lib.copyArray(arr_first, 0, 1);
		
		for(uint i = 1; i < new_indices.length; i++) {
	        new_ind = lib.findPosition_new_element_sort(i - 1, new_indices[i], arr_first_new, arr_second_new, ascending_first, ascending_second);
            if(new_ind < i) {
                new_indices = lib.slice_elements_arr(new_indices, new_ind, i);
                arr_first_new = lib.reorderArr(new_indices, arr_first, 0, i);
                arr_second_new = lib.reorderArr(new_indices, arr_second, 0, i);
            }
            if(i < arr_first.length - 1) {
                arr_first_new[i + 1] = arr_first[i + 1];
                arr_second_new[i + 1] = arr_second[i + 1];
            }
		}
		return new_indices;
	}
	//using insertionsort, sorts a list of OfferBid by price and then quantity
	function insertionSortOffersBidsPrice_Quantity(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price,  bool ascending_quantity) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		uint[] memory new_indices = getInsertionSortIndices(prices, quantities, ascending_price, ascending_quantity);
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
	//using insertionsort, sorts a list of OfferBid by price and then quality
	function insertionSortOffersBidsPrice_Quality(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price,  bool ascending_quality) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory qualities = lib.arr_of_qualities_offerbids(offers_bids);
		uint[] memory new_indices = getInsertionSortIndices(prices, qualities, ascending_price, ascending_quality);
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
	//same as insertionSortOffersBidsPrice_Quality, different version not optimized
	function insertionSortOffersBidsPrice_Quantity_not_optimized(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price,  bool ascending_quantity) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		uint[] memory new_indices = getInsertionSortIndices_not_optimized(prices, quantities, ascending_price, ascending_quantity);
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
	//same as insertionSortOffersBidsPrice_Quality, different version not optimized
	function insertionSortOffersBidsPrice_Quality_not_optimized(Lb.Lib.OfferBid[] memory offers_bids, bool ascending_price,  bool ascending_quality) public view returns(Lb.Lib.OfferBid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory qualities = lib.arr_of_qualities_offerbids(offers_bids);
		uint[] memory new_indices = getInsertionSortIndices_not_optimized(prices, qualities, ascending_price, ascending_quality);
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
}