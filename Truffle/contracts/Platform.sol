pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./Param.sol";
import "./Sorting.sol";
//import "@nomiclabs/buidler/console.sol";

contract Platform {

	/* this is an event which receives one string as argument.
	every event produces a log at the end of a transaction. this event can be used for debugging(not live).
	*/
	event logString(string arg);
	Lb.Lib.OfferBid[] offers;//list of total offers stored. they don't get deleted unless one reset the contract
	Lb.Lib.OfferBid[] bids;//list of total bids stored. they don't get deleted unless one reset the contract
	Lb.Lib.UserInfo[] user_infos;
	Lb.Lib.IdMeter[] id_meters;
	/*list of temporary offers stored. 
	They are relative to each market clearing and for every market clearing they might be deleted. 
	Now the deletion is performed via Web3.py in python before pushing new ones.*/
	Lb.Lib.OfferBid[] tempOffers;
	/*list of temporary bids stored. 
	They are relative to each market clearing and for every market clearing they might be deleted. 
	Now the deletion is performed via Web3.py in python before pushing new ones.*/
	Lb.Lib.OfferBid[] tempBids;
	Lb.Lib.MarketResult[] temp_market_results;//market results for each single clearing(for each specific t_clearing_current)
	Lb.Lib.MarketResultTotal[] market_results_total;//whole market results relative to the whole market clearing(96 loops)
	string public string_to_log = "";//string used for the event logString
	Param p = new Param();//instance of the contract Param
	Lb.Lib lib= new Lb.Lib();//instance of the contract Lib(general library with useful functionalities)
	Sorting srt = new Sorting();//instance of the contract Sorting(useful sorting functionalities)

	constructor() public{
		Platform.clearTempData();//constructor where all the data is cleared.
		}

	function clearTempData() public {//function that deletes objects from the contract storage
	    delete Platform.tempOffers;
		delete Platform.tempBids;
		delete Platform.temp_market_results;
		delete Platform.market_results_total;
	}
	function clearPermanentData() public {//function that deletes objects from the contract storage
		delete Platform.user_infos;
		delete Platform.id_meters;
		delete Platform.offers;
		delete Platform.bids;
	}
	/*
	same function as clearTempData(). It is used when clearTempData() exceeds the gas limit.
	In this case, variables have to be deleted by chunks
	*/
	function clearTempData_gas_limit(uint max_entries) public {
	    for(uint i = 0; i < max_entries; i++){
	    	if(Platform.tempOffers.length > 0){
	    		delete Platform.tempOffers[Platform.tempOffers.length - 1];
	    		Platform.tempOffers.length--;
	    	}
	    	if(Platform.tempBids.length > 0){
	    		delete Platform.tempBids[Platform.tempBids.length - 1];
	    		Platform.tempBids.length--;
	    	}
	    	if(Platform.temp_market_results.length > 0){
	    		delete Platform.temp_market_results[Platform.temp_market_results.length - 1];
	    		Platform.temp_market_results.length--;
	    	}
	    }
	}
	/*
	similar function to clearTempData_gas_limit(). It is used when clearPermanentData() exceeds the gas limit.
	In this case, variables have to be deleted by chunks
	*/
	function clearPermanentData_gas_limit(uint max_entries) public {
	    for(uint i = 0; i < max_entries; i++){
	    	if(Platform.offers.length > 0){
	    		delete Platform.offers[Platform.offers.length - 1];
	    		Platform.offers.length--;
	    	}
	    	if(Platform.bids.length > 0){
	    		delete Platform.bids[Platform.bids.length - 1];
	    		Platform.bids.length--;
	    	}
	    	if(Platform.market_results_total.length > 0){
	    		delete Platform.market_results_total[Platform.market_results_total.length - 1];
	    		Platform.market_results_total.length--;
	    	}
	    	if(Platform.user_infos.length > 0){
	    		delete Platform.user_infos[Platform.user_infos.length - 1];
	    		Platform.user_infos.length--;
	    	}
	    	if(Platform.id_meters.length > 0){
	    		delete Platform.id_meters[Platform.id_meters.length - 1];
	    		Platform.id_meters.length--;
	    	}
	    }
	}
	//add an offer or bid to the list of temporary and/or permanent offers in the storage of the contract
	function pushOfferOrBid(Lb.Lib.OfferBid memory ob, bool isOffer, bool temp, bool permanent) public {
	    if(isOffer) pushOffer(ob, temp, permanent);
		else pushBid(ob, temp, permanent);
	}
	//add an offer to the lists of temporary and/or permanent offers in the storage of the contract
	function pushOffer(Lb.Lib.OfferBid memory off, bool temp, bool permanent) private {
		if(temp) Platform.tempOffers.push(off);
		if(permanent) Platform.offers.push(off);
	}
	//add an bid to the lists of temporary and/or permanent bids in the storage of the contract
	function pushBid(Lb.Lib.OfferBid memory bid, bool temp, bool permanent) private {
		if(temp) Platform.tempBids.push(bid);
		if(permanent) Platform.bids.push(bid);
	}
	//add a user info to the lists of user_infos in the storage of the contract
	function push_user_info(Lb.Lib.UserInfo memory user_info) public {
	    Platform.user_infos.push(user_info);
	}
	//add a id_meter to the lists of id_meters in the storage of the contract
	function push_id_meters(Lb.Lib.IdMeter memory id_meter) public {
		Platform.id_meters.push(id_meter);
	}
	//gets the list of user_infos in the storage of the contract
	function get_user_infos() public view returns (Lb.Lib.UserInfo[] memory) {
		return Platform.user_infos;
	}
	//gets the list of id_meters in the storage of the contract
	function get_id_meters() public view returns (Lb.Lib.IdMeter[] memory) {
		return Platform.id_meters;
	}
	//gets the list of temporary or permanent offers
	function getOffers(bool temp) public view returns(Lb.Lib.OfferBid[] memory) {
	    if(temp) return Platform.tempOffers;
		else return Platform.offers;
	}
	//gets the list of temporary or permanent bids
    function getBids(bool temp) public view returns(Lb.Lib.OfferBid[] memory) {
		if(temp) return Platform.tempBids;
		else return Platform.bids;
    }
    //gets the total market results
	function getMarketResultsTotal() public view returns (Lb.Lib.MarketResultTotal[] memory) {
	    return Platform.market_results_total;
	}
	//gets the temporary market results
	function getTempMarketResults() public view returns (Lb.Lib.MarketResult[] memory) {
	    return Platform.temp_market_results;
	}
}