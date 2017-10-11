function makeSciNumArray(strPairsArray){
    return strPairsArray.map(function (t) { return new SciNum(t[0], t[1]); });
}

function createStatsObj(total, sciNumArr){
    var statsObj = {};
    var arrSum = sumSciNum(sciNumArr);
    statsObj.occurrences = arrSum.toFancyStr();
    statsObj.oneInChance = total.div(arrSum).toFancyStr();
    var pct = arrSum.div(total).mul(new SciNum(1, 2));
    statsObj.pctChance = pct.toFancyStr();
    statsObj.total = total.toFancyStr();
    return statsObj;
}

function createSciNumObj(rollToMantissaPowerArr) {
    var outObj = {};
    var forTotal = [];
    for (var roll in rollToMantissaPowerArr){
        if (rollToMantissaPowerArr.hasOwnProperty(roll)) {
            var manPowArr = rollToMantissaPowerArr[roll];
            var number = new SciNum(manPowArr[0], manPowArr[1]);
            outObj[roll] = number;
            forTotal.push(number);
        }
    }
    outObj["total"] = sumSciNum(forTotal);
    return outObj;
}

function getStats(sciNumObj, numArr) {
    var sciNumArr = numArr.map(function (t) { return getSciNumValue(sciNumObj, t); });
    return createStatsObj(sciNumObj.total, sciNumArr);
}

function getSciNumValue(obj, key) {
    return obj.hasOwnProperty(key) ? obj[key] : new SciNum(0, 0);
}