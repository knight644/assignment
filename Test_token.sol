// SPDX-License-Identifier: MIT
pragma solidity >=0.4.22 <0.8.0;

contract Testtoken {
    /* Create an array with balances*/
    mapping (address => uint256) public balanceOf;

    /* Give the creator all the tokens */
    constructor (uint256 initialSupply) public{
        balanceOf[msg.sender] = initialSupply;
    }

    /* Send tokens */
    function transfer (address _to, uint256 _value) public{
        // Check if the sender has enough
        if (balanceOf[msg.sender] < _value) revert();

        //check for overflow
        if (balanceOf[_to] + _value < balanceOf[_to]) revert();

        // deduct from sender
        balanceOf[msg.sender] -= _value;

        // add the same to recepient
        balanceOf[_to] += _value;
    }
}