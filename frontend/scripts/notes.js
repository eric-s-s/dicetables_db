var queries = {
    a: 0,
    b: 0,
    c: 0
};

function computeRoot() {
    var a = document.querySelector("#aVal").value;
    var b = document.querySelector("#bVal").value;
    var c = document.querySelector("#cVal").value;
    var delta = Math.pow(b, 2) - 4*a*c;
    var base = -b;
    var divisor = 2*a;
    if (delta < 0 || a === 0){
        answer = "none";
    } else if (delta > 0){
        var discriminant = Math.sqrt(delta);
        answer = (base + discriminant) / divisor + " or " + (base - discriminant) / divisor
            + " delta " +delta +a;
    } else {
        answer = base / divisor + " delta " +delta+a;
    }
    document.querySelector("#result").innerHTML = answer;
}

function updateLabel(){
    var a = document.querySelector("#aVal").value;
    var b = document.querySelector("#bVal").value;
    var c = document.querySelector("#cVal").value;
    outputStr = a + "x^2 + " + b + "x + " + c;
    document.querySelector("#equation").innerHTML = outputStr;
}





function sumSciNum(sciNumArray) {
    var maxPow = maxPower(sciNumArray);
    var maxPowerDiff = 15;
    var newMantissa = 0;
    var arrayLen = sciNumArray.length;
    for (var i=0; i < arrayLen; i++) {
        var theNum = sciNumArray[i];
        var powerDiff = maxPow - theNum.power;
        if (powerDiff <= maxPowerDiff){
            newMantissa += theNum.mantissa * Math.pow(10, -powerDiff);
        }
    }
    while (newMantissa >= 10) {
        newMantissa /= 10;
        maxPow += 1;
    }
    return new SciNum(newMantissa, maxPow);
}


function maxPower(sciNumArray) {
    return Math.max.apply(null, sciNumArray.map(function (el){return el.power;}))
}



var x = new SciNum(1.23, 466);
var y = new SciNum(3.45, 468);
var z = new SciNum(1.24, 500);
console.log(x.add(y));
console.log(y.add(x));
console.log(z.add(x));
console.log(x.add(z));

console.log(z.div(x));
console.log(x.div(z));
console.log(x.div(y));
console.log(y.div(x));

var x = new SciNum(1.23, -266);
var y = new SciNum(1.23, -268);
var z = new SciNum(1.23, -266);
console.log(x.add(y));
console.log(y.add(x));
console.log(z.add(x));
console.log(x.add(z));

console.log(z.div(x));
console.log(x.div(z));
console.log(x.div(y));
console.log(y.div(x));
console.log(sumSciNum([x, y, z]));
// make lst of SciNum from array of num
var lst = [["9.999999", "23"], [2, 25]];
var sn = lst.map(function (el) {return new SciNum(el[0], el[1]);});
console.log(lst);
console.log(sn);
console.log(sn[0].add(sn[1]));
console.log(sn.map(function (t) {return t.toString(); }));
