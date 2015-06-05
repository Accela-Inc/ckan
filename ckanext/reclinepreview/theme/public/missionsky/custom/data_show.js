//global fields & records
var gfields;
var grecords;

// graph fields & records
var graphFields;
var graphRecords;

var iframeModel;
var lastQuery;
var init = true;
// var resourcePrivate = false;
var resourceId = preload_resource.id;

// grid
var grid;
var currentSortCol = null;
var isAsc = null;

// chart
var seriesX = "";
var seriesY = new Array();
var chart;

// map
var map;
var featureGroup;
var initMap = true;
var noBoundery = true;

$.support.cors = true;
var el = {
	endPoint: $('body').data('site-root'),
	alertMsg: $("#alert-messages"),
	sclickGrid: $("#slickgrid"),
	visualizeBtnGroup: $("#visualize-btn-group"),
	toSlickGrid: $("#to-slickgrid"),
	toGraph: $("#to-graph"),
	toLeaflet: $("#to-leaflet"),
	visualizeBtn: $("#visualize-btn"),
	filterBtn: $("#filter-btn"),
	graph: $("#graph"),
	leaflet: $("#leaflet"),
	filterField: $("#filter-field"),
	menuBtnGroup: $("#menu-btn-group"),
	menuBtn: $("#menu-btn")
}

$(function() {
	el.toSlickGrid.click(function() {
		el.alertMsg.hide();
		$(".data-view-container>div").not("#slickgrid").hide();
		el.visualizeBtnGroup.hide();
		el.menuBtnGroup.hide();
		el.sclickGrid.show();
		if (el.filterBtn.hasClass("active")) {
			$(".filter-panel").show();
			$(".data-view-sidebar").show();
		} else {
			$(".data-view-sidebar").hide();
			el.sclickGrid.css("width", "100%");
		}
	});

	el.toGraph.click(function() {
		el.alertMsg.hide();
		$(".data-view-container>div").not("#graph").hide();
		el.graph.show();
		el.visualizeBtnGroup.show();
		el.menuBtnGroup.hide();
		if (el.filterBtn.hasClass("active")) {
			$(".filter-panel").show();
			$(".data-view-sidebar").show();
		} else {
			if (!el.visualizeBtn.hasClass("active")) {
				el.visualizeBtn.click();
			} else {
				$(".graph-panel").show();
				$(".filter-panel").hide();
				$(".data-view-sidebar").show();
			}
		}
	});

	el.toLeaflet.click(function() {
		$(".data-view-container>div").not("#leaflet").hide();
		el.leaflet.show();
		el.visualizeBtnGroup.hide();
		el.menuBtnGroup.show();
		if ($(".graph-panel").css("display") === "block") {
			$(".data-view-sidebar").hide();
		}
		//if (initMap) {
			createMap();
		//} else {
			if (noBoundery) {
				fitMapBounds();
			}
		//}
	});

	el.toLeaflet.one("mouseover", function() {
	    if(grecords.length > 0) {
	        var html = "";
            for(key in grecords[0]) {
                html += '<label><input type="checkbox" name="menus" value="'+key+'" checked> '+key+'</label>';
            }
            $("#columns-form").html(html);
	    }

        var el = {
            checkAll: $("#checked-all"),
            menus: $("input[name=menus]")
        }
        el.checkAll.click(function() {
            el.menus.prop("checked", this.checked);
        });
	    el.menus.click(function() {
	       el.checkAll.prop("checked", el.menus.length == $("input[name=menus]:checked").length);
	    });
	});

	el.menuBtn.click(function(e) {
	    if($(e.target).attr("disabled") != "disabled") {
	        if ($(e.target).hasClass("active")) {
                $(".filter-panel").hide();
                $(".graph-panel").hide();
                $("#map-panel").hide();
                $(".data-view-sidebar").hide();
                el.sclickGrid.css("width", "100%");
            } else {
                el.filterBtn.removeClass("active");
                el.visualizeBtn.removeClass("active");

                $("#map-panel").show();
                $(".graph-panel").hide();
                $(".filter-panel").hide();
                $(".data-view-sidebar").show();
                el.sclickGrid.css("width", "628px");
            }
	    }
	});

	el.visualizeBtn.click(function(e) {
		if ($(e.target).attr("disabled") != "disabled") {
			if ($(this).hasClass("active")) {
				$(".filter-panel").hide();
                $(".graph-panel").hide();
                $("#map-panel").hide();
                $(".data-view-sidebar").hide();
                el.sclickGrid.css("width", "100%");
			} else {
				el.filterBtn.removeClass("active");
				el.menuBtn.removeClass("active");

				$(".graph-panel").show();
				$(".filter-panel").hide();
				$("#map-panel").hide();
				$(".data-view-sidebar").show();
				el.sclickGrid.css("width", "628px");
			}
		}
	});

	el.filterBtn.click(function(e) {
		if ($(e.target).hasClass("active")) {
			$(".filter-panel").hide();
			$(".graph-panel").hide();
			$("#map-panel").hide();
			$(".data-view-sidebar").hide();
			el.sclickGrid.css("width", "100%");
		} else {
            el.visualizeBtn.removeClass("active");
            el.menuBtn.removeClass("active");

			$(".filter-panel").show();
			$(".graph-panel").hide();
			$("#map-panel").hide();
			$(".data-view-sidebar").show();
			el.sclickGrid.css("width", "628px");
		}
	});

	el.filterField.change(function() {
		var filterFieId = $(this).val();
		var fieldType, findex, filterTypeHTML;
		$.each(gfields,function(index, field){
			if (field.id === filterFieId) {
				fieldType = field.type;
				findex = index;
				return false;
			}
		});

		switch (fieldType) {
			case "text":
				filterTypeHTML = "<option value='term'>Equal</option><option value='contain'>Contain</option>";
				break;
			case "number":
				filterTypeHTML = "<option value='term'>Equal</option><option value='range'>Range</option>";
				break;
			case "numeric":
				filterTypeHTML = "<option value='term'>Equal</option><option value='range'>Range</option>";
				break;
			case "date" :
				$("#filter-field-type").attr("format", gfields[findex].format);
				filterTypeHTML = "<option value='dateterm'>Equal</option><option value='daterange'>Date Range</option>";
				break;
			case "timestamp":
				filterTypeHTML = "<option value='timestampterm'>Equal</option><option value='timestamprange'>Date Range</option>";
				break;
			default:
				filterTypeHTML = "<option value='term'>Equal</option><option value='contain'>Contain</option>";
		}
		$("#filter-field-type").html(filterTypeHTML);
		$("#filter-field-type").removeAttr("disabled");
		$("#add-filter-btn").removeAttr("disabled");
	});

	$("#group-by-field").change(function() {
		if ($($(".current-chart")[0]).attr("for") === "pie3d") {
			createPie(true);
		} else {
			createPie();
		}
	});

	$("#chart-type-title").click(function() {
		$("#chart-types").toggle("normal", "linear");
	});

	$("#add-filter-title").click(function() {
		$("#add-filter").toggle("normal", "linear");
	});

	$("#filters-title").click(function() {
		$("#filters").toggle("normal", "linear");
	});

	$("#column-definition-title").click(function() {
		$("#column-definition").toggle("normal", "linear");
	});

	$("#chart-embed-title").click(function() {
		$("#chart-embed").toggle("normal", "linear");
	});

	$("#advanced-data-title").click(function() {
		$("#advanced-data").toggle("normal", "linear");
	});

	$("#show-column-title").click(function(){
		$("#show-column").toggle("normal", "linear");
	});

	$("#geocode-auto").click(function(){
		$("#long").attr("disabled", "disabled");
		$("#lat").attr("disabled", "disabled");
	});

	$("#geocode-manu").click(function(){
		$("#long").removeAttr("disabled");
		$("#lat").removeAttr("disabled");
	});

	$(".tree3").click(function() {
		var iconspan = $(this).find("span.iconspan");
		iconspan.toggleClass("tree3-span-down");
	});

	iframeModel = (getUrlParam("iframe") != null);

	if (!iframeModel) {
		var packageId = $("#data-organization-id", window.parent.document).attr('data-package-name');
		$.ajax({
			type : "POST",
			url : el.endPoint + 'api/3/action/package_show?id=' + packageId,
			async : false,
			success: function(data){
				if(data.result.private) {
					$("#chart-embed-title").remove();
					$("#chart-embed").remove();
				}
				search();
			}
		});
	} else {
		initLimit(Number(getUrlParam("offset")), Number(getUrlParam("limit")));
		if(getUrlParam("currentsortcol") != null) {
			currentSortCol = JSON.parse(window.decodeURI(getUrlParam("currentsortcol")));
			isAsc = (getUrlParam("isasc") == "true");
		}

		if (getUrlParam("filters") != null) {
			gfields = JSON.parse(window.decodeURI(getUrlParam("gfields")));
			filter();
		} else {
			$("#searchQ").val(getUrlParam("searchq"));
			search();
		}
	}

	$(".chartType").click(function() {
		$("#sliders").hide();
		var thisLabel = $(this).find("label")[0];
		$(".current-chart").removeClass("current-chart");
		$(thisLabel).addClass("current-chart");
		reDrawGraph();
	});

	$("#group-column-type").change(function(){
		var columnType = $(this).val();
		if (columnType === "groupby" && $("#group-column").val() === "") {
			$("#group-column").val("_id");
		}
		var $filedsets = $("#series").find("fieldset");
        $filedsets.not($filedsets.eq(0)).remove();
        $("#add-series-btn").click().removeAttr("disabled");
		reDrawGraph();
		seriesGear();
	});

	$("#embed-type").change(function(){
		createEmbedCode();
	});
});

function getUrlParam(name) {
	var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)");
	var r = window.location.search.substr(1).match(reg);
	return (r != null)? unescape(r[2]): null;
}

function showGraph(graphType, seriesX, seriesY) {
	if (graphType === "pie" || graphType === "pie3d") {
		$("#series").hide();
		$("#group-by").show();
	} else {
		$("#series").show();
		$("#group-by").hide();
	}

	switch (graphType) {
	case "line":
		createLineChart(seriesX, seriesY);
		break;
	case "areaSpline":
		createAreasplineChart(seriesX, seriesY);
		break;
	case "column":
		createColumnChart(seriesX, seriesY);
		break;
	case "column3d":
		create3DColumnChart(seriesX, seriesY);
		break;
	case "bar":
		createBar(seriesX, seriesY);
		break;
	case "pie":
		createPie();
		break;
	case "pie3d":
		create3DPie();
		break;
	}
}

function search() {
	searchData().done();
	lastQuery = "search";
}

function searchData() {
	var params = new Object();
	var filters = new Object();
	var offset = Number($("#searchOffset").val());
	params.resource_id = resourceId;
	params.q = sqlReplace($("#searchQ").val());
	params.filters = filters;
	params.offset = offset === 0 ? 0 : offset - 1;
	params.limit = Number($("#searchEnd").val()) - params.offset;
	if (currentSortCol != null) {
		params.sort = currentSortCol.id + (isAsc ? " asc" : " desc");
	} else {
		params.sort = "_id asc";
	}
	var url = el.endPoint + "api/3/action/datastore_search";
	return $.post(url, JSON.stringify(params), function(data) {
		processData(data);
	});
}

function prevSearch() {
	var _offset = $("#searchOffset"), _end = $("#searchEnd");
	var offset = Number(_offset.val()), end = Number(_end.val());
	var result = end - offset;
	if (result != 99 && offset - result >= 0) {
		_offset.val(offset - result - 1);
		_end.val(end - result - 1);
		filter();
	} else if ((offset - 100) >= 0) {
		$("#searchEnd").val(offset - 1);
		$("#searchOffset").val(offset - 100);

		if (lastQuery != "filter") {
			search();
		} else {
			filter();
		}
	} else if (offset > 1) {
		$("#searchEnd").val(100);
		$("#searchOffset").val(1);
		// if (resourcePrivate) {
		// 	search();
		// } else {
		// 	filter();
		// }
		if (lastQuery != "filter") {
			search();
		} else {
			filter();
		}
	}
}

function nextSearch() {
	var _offset = $("#searchOffset"), _end = $("#searchEnd");
	var offset = Number(_offset.val()), end = Number(_end.val()), max = Number($("#doc-count").html());
	var result = end - offset;

	if (end < max) {
		if (result != 99) {
			if (max - end >= result) {
				_offset.val(end + 1);
				_end.val(end + result + 1);
			} else {
				_offset.val(end + 1);
				_end.val(max);
			}
		} else {
			_offset.val(offset + 100);
			_end.val(offset + 199);
		}

		if (lastQuery === "search") {
			search();
		} else {
			filter();
		}
	}
}

function doFilter() {
	initLimit(0, 100);
	filter();
}

function filter() {
	var url = el.endPoint + "api/3/action/datastore_search_sql?sql=" + getFilterSQL(true);
	$.post(url, function(data) {
		processData(data);
	}).done();
	lastQuery = "filter";
}

function getFilterSQL(NeedLimit) {
	var sql = 'SELECT ';
	$.each(gfields, function(index, field){
		if (field.id != "_full_count" && field.id != "rank" && field.id != "_full_text") {
			sql = sql + '"' + field.id + '",';
		}
	});
	sql = sql.replace(new RegExp(",$"), "") + ' FROM "' + resourceId
			+ '" WHERE 1=1';
	var filters = iframeModel && getUrlParam("filters") != null ? JSON.parse(window.decodeURI(getUrlParam("filters"))) : getFilters();
	$.each(filters, function(index, filter) {
		if (filter.type === 'range') {
			if (filter.from != '') {
				if (getFieldType(filter.id) === "numeric") {
					sql = sql + ' AND "' + filter.id + '" >= ' + filter.from;
				} else {
					sql = sql + ' AND "' + filter.id
						+ '" != \'\' AND to_number("' + filter.id
						+ '", \'999999999999D9999\') >= ' + filter.from;
				}
			}
			if (filter.to != '') {
				if (getFieldType(filter.id) === "numeric") {
					sql = sql + ' AND "' + filter.id + '" <= ' + filter.to;
				} else {
					sql = sql + ' AND "' + filter.id
						+ '" != \'\' AND to_number("' + filter.id
						+ '", \'999999999999D9999\') <= ' + filter.to;
				}
			}
		} else if (filter.type === 'daterange') {
			if (filter.from != '') {
				sql = sql + ' AND to_date("' + filter.id + '",\''
						+ filter.format + '\') >= to_date(\'' + filter.from
						+ '\',\'mm/dd/yyyy\')';
			}
			if (filter.to != '') {
				sql = sql + ' AND to_date("' + filter.id + '",\''
						+ filter.format + '\') <= to_date(\'' + filter.to
						+ '\',\'mm/dd/yyyy\')';
			}
		} else if (filter.type === 'dateterm') {
			sql = sql + ' AND to_date("' + filter.id + '",\'' + filter.format
					+ '\') = to_date(\'' + filter.value + '\',\'mm/dd/yyyy\')';
		} else if (filter.type === 'timestamprange') {
			if (filter.from != '') {
				sql = sql + ' AND "' + filter.id + '" >= to_date(\''
						+ filter.from + '\',\'mm/dd/yyyy\')';
			}
			if (filter.to != '') {
				sql = sql + ' AND "' + filter.id + '" <= to_date(\''
						+ filter.to + '\',\'mm/dd/yyyy\')';
			}
		} else if (filter.type === 'timestampterm') {
			sql = sql + ' AND "' + filter.id + '" = to_date(\'' + filter.value
					+ '\',\'mm/dd/yyyy\')';
		} else if (filter.type === 'term') {
			if (getFieldType(filter.id) === "numeric") {
				sql = sql + ' AND "' + filter.id + '" = ' + filter.to;
			} else {
				sql = sql + ' and "' + filter.id + '"=E\''
						+ valReplace(filter.value) + '\'';
			}
		} else if (filter.type === 'contain') {
			sql = sql + ' and "' + filter.id + '" ILIKE E\'%!'
					+ valReplace(filter.value) + '%\'ESCAPE \'!\'';
		}
	});
	if (currentSortCol != null) {
		sql = sql + ' ORDER BY "' + currentSortCol.id + '"' + (isAsc?' ASC ':' DESC ');
	} else {
		sql = sql + ' ORDER BY "_id" ASC ';
	}

	if (NeedLimit) {
		var offset = iframeModel ? Number(getUrlParam("offset")) : Number($("#searchOffset").val());
		var end = Number($("#searchEnd").val());

		if (!iframeModel && offset == 0) {
			$("#searchOffset").val(1);
			offset = Number($("#searchOffset").val());
		}
		if (end == 0) {
			$("#searchEnd").val(100);
			end = Number($("#searchEnd").val());
		}

		var limit =iframeModel ? Number(getUrlParam("limit")): (end - offset + 1);
		if (limit <= 0) {
			limit = 100;
			$("#searchEnd").val(offset + 99);
		}
		offset = offset === 0 ? 0 : (iframeModel ? offset : (offset - 1));
		sql = sql + ' LIMIT ' + limit + ' OFFSET ' + offset;
	}
	return sql;
}

function initLimit(offset, limit) {
	$('#searchOffset').val(offset + 1);
	$('#searchEnd').val(offset + limit);
}

function getFilters() {
	var filters = new Array();
	var filter;
	var tempFilterID;
	var tempFilterType;
	$("#filters").find("fieldset").each(function(index, fieldset) {
		filter = new Object();
		tempFilterID = $(this).attr("forField");
		tempFilterType = $(this).attr("forType");
		filter.id = tempFilterID;
		filter.type = tempFilterType;

		switch (tempFilterType) {
		case "term":
			if (getFieldType(filter.id) === "numeric") {
				filter.to = $(this).find("input[name='term']")[0].value;
			} else {
				filter.value = $(this).find("input[name='term']")[0].value;
			}
			break;
		case "contain":
			filter.value = $(this).find("input[name='contain']")[0].value;
			break;
		case "range":
			filter.from = $(this).find("input[name='range-from']")[0].value;
			filter.to = $(this).find("input[name='range-to']")[0].value;
			break;
		case "dateterm":
			filter.value = $(this).find("input[name='date-term']")[0].value;
			filter.format = $(this).attr("dateformat");
			break;
		case "daterange":
			filter.from = $(this).find("input[name='range-from']")[0].value;
			filter.to = $(this).find("input[name='range-to']")[0].value;
			filter.format = $(this).attr("dateformat");
			break;
		case "timestampterm":
			filter.value = $(this).find("input[name='date-term']")[0].value;
			break;
		case "timestamprange":
			filter.from = $(this).find("input[name='range-from']")[0].value;
			filter.to = $(this).find("input[name='range-to']")[0].value;
			break;
		}
		filters.push(filter);
	});
	return filters;
}

function getFieldType(key) {
	var type;
	$.each(gfields, function(index, f) {
		if (f.id === key) {
			type = f.type;
			return false;
		}
	});
	return type;
}

function addAnFilter() {
	var filterField = el.filterField.val();
	if (filterField != "" && filterField != null) {
		var filterType = $("#filter-field-type").val();
		var filterHTML;
		switch (filterType) {
		case "term":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='term'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>Equal</label><input name='term' type='text'></fieldset>";
			break;
		case "contain":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='contain'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>Contain</label><input name='contain' type='text'></fieldset>";
			break;
		case "range":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='range'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>From</label><input name='range-from' type='text'><label>To</label><input name='range-to' type='text'></fieldset>";
			break;
		case "dateterm":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='dateterm' dateformat='"
					+ $("#filter-field-type").attr("format")
					+ "'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>Equal</label><input name='date-term' type='text'></fieldset>";
			break;
		case "daterange":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='daterange' dateformat='"
					+ $("#filter-field-type").attr("format")
					+ "'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>From</label><input name='range-from' type='text'><label>To</label><input name='range-to' type='text'></fieldset>";
			break;
		case "timestampterm":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='timestampterm'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>Equal</label><input name='date-term' type='text'></fieldset>";
			break;
		case "timestamprange":
			filterHTML = "<fieldset forField='"
					+ filterField
					+ "' forType='timestamprange'><legend style='font-size: 14px; margin-bottom: 2px;'> "
					+ filterField
					+ " <small>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</small><a class='removeFilter'>×</a></legend><label>From</label><input name='range-from' type='text'><label>To</label><input name='range-to' type='text'></fieldset>";
			break;
		}
		if ($("#filters").html() === "") {
			$("#filters").append("<input type='button' id='do-filter-btn' value='Filter' style='width: 120px;' class='btn' onclick='javascript:doFilter();'>");
		};
		$("#do-filter-btn").before(filterHTML);

		if (filterType === "daterange" || filterType === "timestamprange") {
			var lastFieldset = $("#filters").find("fieldset").last();
			var lastRangeFrom = lastFieldset.find("input[name='range-from']")[0];
			var lastRangeTo = lastFieldset.find("input[name='range-to']")[0];
			$(lastRangeFrom).datepicker({changeMonth: true, changeYear: true});
			$(lastRangeTo).datepicker({changeMonth: true, changeYear: true});
		}

		if (filterType === "dateterm" || filterType === "timestampterm") {
			var lastFieldset = $("#filters").find("fieldset").last();
			var lastDateTerm = lastFieldset.find("input[name='date-term']")[0];
			$(lastDateTerm).datepicker({changeMonth: true, changeYear: true});
		}

		if ($("#filters").css("display") === "none") {
			$("#filters-title").click();
		}
		$(".removeFilter").click(function() {
			$(this).parent().parent().remove();
			if ($("#filters").find("fieldset").length === 0) {
				$("#do-filter-btn").remove();
			}
			filter();
		});
	}
}

function addSeries() {
	var words = [ 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
			'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
			'Z' ];
	var seriesNum = $("#series").find("fieldset").length;
	var seriesHTML;
	var groupColumnType = $("#group-column-type").val();
	if (groupColumnType === "value") {
		seriesHTML = "<fieldset><label><span class='series-span'>Series "
				+ words[seriesNum - 1]
				+ "(Y-Axis)</span>[<a>Remove</a>]</label><select name='series-type' class='series-type' style='width:80px;margin-right:2px;'><option value='value'>Value</option></select><select name='series-field' class='series-field' style='width:108px;'>";
	} else {
		seriesHTML = "<fieldset><label><span class='series-span'>Series "
				+ words[seriesNum - 1]
				+ "(Y-Axis)</span>[<a>Remove</a>]</label><select name='series-type' class='series-type' style='width:80px;margin-right:2px;'><option value='count'>Count</option><option value='sum'>Sum</option><option value='max'>Max</option><option value='min'>Min</option><option value='avg'>Avg</option></select><select name='series-field' class='series-field' style='width:108px;'>";
	}

	var seriesY = new Array();
	var seriesFieldSets = $("#series").find("fieldset");
	$.each(gfields, function(index, field) {
		if (field.id != "_full_count" && field.id != "rank") {
			if (groupColumnType != "value" || field.type === "number"
					|| field.type === "int4" || field.type === "numeric") {
				seriesY.push(field);
			}
		}
	});
	if (seriesY.length === (seriesFieldSets.length + 1)) {
		$("#add-series-btn").attr({"disabled": "disabled"});
	}

	$.each(seriesFieldSets, function(index, series){
		var seriesVal = $($(this).find("select[class='series-field']")[0]).val();
		$.each(seriesY, function(index, y){
			if (y.id === seriesVal) {
				seriesY.splice(index, 1);
				return false;
			}
		});
	});

	$.each(seriesY, function(index, y) {
		if (y.id != "_id") {
			seriesHTML = seriesHTML + "<option value='" + y.id + "'>" + y.id
					+ "</option>";
		}
	});
	$("#add-series-btn").before(seriesHTML + "</select></fieldset>");

	$("#series>fieldset>label>a").click(function() {
		$(this).parent().parent().remove();
		$("#add-series-btn").removeAttr("disabled");
		reDrawGraph();
        seriesGear();
	});

	reDrawGraph();
	seriesGear();

	$(".series-field").change(function() {
		seriesGear();
		reDrawGraph();
	});

	$(".series-type").change(function() {
		seriesGear();
		reDrawGraph();
	});
}

function seriesGear() {
	var seriesType;
	var seriesSelect;
	var seriesVal;
	var seriesHTML;
	var seriesY = new Array();
	var seriesFieldSets = $("#series").find("fieldset");

	$.each(gfields, function(index, field) {
		if(field.id != "_full_count" && field.id != "rank") {
			seriesY.push(field);
		}
	});
	$.each(seriesFieldSets, function(index, series){
		var seriesVal = $($(this).find("select[class='series-field']")[0]).val();
		$.each(seriesY, function(index, y){
			if (y.id === seriesVal) {
				seriesY.splice(index, 1);
				return false;
			}
		});
	});

	$.each(seriesFieldSets,function(index, sfs) {
		seriesType = $($(this).find("select[class='series-type']")[0]).val();
		seriesSelect = $($(this).find("select[class='series-field']")[0]);
		seriesHTML = "";
		seriesVal = seriesSelect.val();
		if (index === 0) {
			var columnType = $("#group-column-type").val();
			if (seriesVal === "" && columnType != "groupby") {
				seriesHTML = "<option selected='selected' value=''>No column selected</option>";
			} else {
				seriesHTML = "<option value='"
					+ seriesVal
					+ "' selected='selected'>"
					+ seriesVal
					+ "</option>"
					+ (columnType != "groupby" ? "<option value=''>No column selected</option>": "");
			}

			$.each(seriesY,function(i, y){
				seriesHTML = seriesHTML + "<option value='" + y.id + "'>" + y.id + "</option>";
			});
		} else {
			var fieldType = getFieldType(seriesVal);
			if (seriesType === "count"
					|| fieldType === "number"
					|| fieldType === "int4"
					|| fieldType === "numeric") {
				seriesHTML = "<option value='" + seriesVal
					+ "' selected='selected'>" + seriesVal
					+ "</option>";
			}

			$.each(seriesY, function(i, y) {
                if ((y.id != "_id") && (seriesType === "count"
                        || y.type === "number"
                        || y.type === "int4"
                        || y.type === "numeric")) {
                    seriesHTML = seriesHTML
                        + "<option value='" + y.id
                        + "'>" + y.id + "</option>";
                }
			});
		}

		if (seriesHTML != "") {
			seriesSelect.html(seriesHTML);
		} else {
			$(this).remove();
			/*$("#add-series-btn").attr("disabled", "disabled");*/
		}

		var groupColumn = $("#group-column");
		if ((groupColumn.val() != "_id" && groupColumn
				.find("option").length === 2)
				|| groupColumn.find("option").length === 1) {
			$("#add-series-btn").attr("disabled", "disabled");
		}
	});
}

function reDrawGraph() {
	seriesX = iframeModel ? getUrlParam("groupcolumn") : $("#group-column").val();
	seriesY = new Array();
	var syList = new Array();
	var sy;
	var advancedGraph = false;
	if (iframeModel) {
		$("body").css("margin", "0px");
		$("body").css("padding", "0px");
		$(".header").hide();
		$("#to-graph").click();
		$(".data-view-sidebar").hide();
		if(getUrlParam("charttype") != "pie" && getUrlParam("charttype") != "pie3d") {
			syList = JSON.parse(window.decodeURI(getUrlParam("fields")));
			$.each(syList,function(index, sy){
				if (sy.type != "value") {
					seriesY.push(sy.type.toUpperCase() + "(" + sy.name + ")");
					advancedGraph = true;
				} else {
					seriesY.push(sy.name);
				};
			});
		}
	} else {
		$.each($("#series").find("fieldset"), function(index, series) {
			if (index != 0) {
				sy = new Object();
				sy.type = $(this).find("select[name='series-type']")[0].value;
				sy.name = $(this).find("select[name='series-field']")[0].value;
				syList.push(sy);
				if (sy.type != "value") {
					seriesY.push(sy.type.toUpperCase() + "(" + sy.name + ")");
					advancedGraph = true;
				} else {
					seriesY.push(sy.name);
				}
			}
		});
	}
	// if (!resourcePrivate) {
	$.each(gfields, function(index, field) {
		if (init && field.id === "STATUS") {
			$(".current-chart").removeClass("current-chart");
			$($("#pie-div").find("label")[0]).addClass("current-chart");
			$("#group-by-field").val("STATUS");
			init = false;
			return false;
		}
	});

	if (init) {
		init = false;
	}

	if (grecords.length > 0 && advancedGraph) {
		var sql;
		var fieldStr;
		var fieldType;
		var _id = 'GREATEST(';
		var sqlPrefix = 'SELECT ';
		var sqlSuffix = '';

		var offset = iframeModel ? getUrlParam("offset") : Number($("#searchOffset").val()) - 1;
		var limit = iframeModel ? getUrlParam("limit") : Number($("#searchEnd").val()) - offset;
		if (limit <= 0) {
			limit = 100;
			$("#searchEnd").val(offset + 100);
		}

		var xAxisType = iframeModel ? getUrlParam("groupcolumntype") : $("#group-column-type").val();
		var groupByField = "";
		if (xAxisType === "groupby") {
			groupByField = '"' + seriesX + '"';
		};
		var datasource = ((iframeModel && getUrlParam("filters") != null) || $("#filters").find("fieldset").length > 0 || currentSortCol != null)? '(' + getFilterSQL(true) + ') ds ' : '"' + resourceId + '" ORDER BY "' + seriesX + '" ASC OFFSET ' + offset + ' LIMIT ' + limit;

		$.each(syList, function(index,s){
			_id = _id + 't' + index + '.rn,';
			fieldType = getFieldType(s.name);

			fieldStr = (fieldType != "number" || fieldType === "numeric" || fieldType === "int4")?'"' + s.name + '"':'NULLIF ("' + s.name + '", \'\') :: NUMERIC';
			if (xAxisType != "groupby") {
				groupByField = fieldStr;
			}
			if (index != 0) {
				sqlSuffix = sqlSuffix + ' FULL JOIN ';
			}
			switch (s.type) {
				case "value":
					sqlPrefix = sqlPrefix + 't' + index + '."' + s.name + '",';
					sqlSuffix = sqlSuffix + '(SELECT "'+ s.name +'",' + groupByField + ' AS rn FROM "' + resourceId + '" GROUP BY "' + s.name + '",rn OFFSET ' + offset + ' LIMIT ' + limit + ') t' + index;
					break;
				case "count":
					sqlPrefix = sqlPrefix + 't' + index + '."COUNT(' + s.name + ')",';
					sqlSuffix = sqlSuffix + '(SELECT COUNT(*) as "COUNT(' + s.name + ')",' + groupByField + ' AS rn FROM (SELECT * FROM ' + datasource + ') t' + index + index + ' GROUP BY rn) t' + index;
					break;
				case "sum":
					sqlPrefix = sqlPrefix + 't' + index + '."SUM(' + s.name + ')",';
					sqlSuffix = sqlSuffix + '(SELECT SUM(' + fieldStr + ') as "SUM(' +  s.name + ')",' + groupByField + ' AS rn FROM (SELECT * FROM ' + datasource + ') t' + index + index + ' GROUP BY rn) t' + index;
					break;
				case "max":
					sqlPrefix = sqlPrefix + 't' + index + '."MAX(' + s.name + ')",';
					sqlSuffix = sqlSuffix + '(SELECT MAX(' + fieldStr + ') as "MAX(' + s.name + ')",' + groupByField + ' AS rn FROM (SELECT * FROM ' + datasource + ') t' + index + index + ' GROUP BY rn) t' + index;
					break;
				case "min":
					sqlPrefix = sqlPrefix + 't' + index + '."MIN(' + s.name + ')",';
					sqlSuffix = sqlSuffix + '(SELECT MIN(' + fieldStr + ') as "MIN(' + s.name + ')",' + groupByField + ' AS rn FROM (SELECT * FROM ' + datasource + ') t' + index + index + ' GROUP BY rn) t' + index;
					break;
				case "avg":
					sqlPrefix = sqlPrefix + 't' + index + '."AVG(' + s.name + ')",';
					sqlSuffix = sqlSuffix + '(SELECT AVG(' + fieldStr + ') as "AVG(' + s.name + ')",' + groupByField + ' AS rn FROM (SELECT * FROM ' + datasource + ') t' + index + index + ' GROUP BY rn) t' + index;
					break;
			}

			if (index >= 1) {
				sqlSuffix = sqlSuffix + ' ON t' + (index - 1) + '.rn = t' + index + ".rn ";
			};
		});

		sqlPrefix = sqlPrefix + _id;
		sqlPrefix = sqlPrefix.replace(new RegExp(',$'),'') + ') AS ' + (xAxisType!='groupby'?'_id':('"'+seriesX + '"')) + ' FROM ';
		// sql = sqlPrefix + sqlSuffix + ' ORDER BY ' + (xAxisType!='groupby'?'_id':('"'+seriesX + '"')) + ' ASC LIMIT 500';
		sql = sqlPrefix + sqlSuffix + (seriesX === (currentSortCol != null ?currentSortCol.id : null) ? (' ORDER BY "' + seriesX + '"' + (isAsc?' ASC ':' DESC ')) : '') + ' LIMIT 500';
		
		$.post( el.endPoint + "api/3/action/datastore_search_sql?sql=" + sql,
			function(data) {
				graphFields = data.result.fields;
				graphRecords = data.result.records;
				showGraph(iframeModel ? getUrlParam("charttype") : $(
					$(".current-chart")[0]).attr("for"), seriesX,
					seriesY);
			}).done();
	} else {
		graphFields = null;
		graphRecords = null;
		showGraph(iframeModel ? getUrlParam("charttype") : $($(".current-chart")[0]).attr("for"), seriesX, seriesY);
	};
	// } else {
	// 	if (init) {
	// 		init = false;
	// 	}
	// 	showGraph(iframeModel ? getUrlParam("charttype") : $($(".current-chart")[0]).attr("for"), seriesX, seriesY);
	// }
}

function createEmbedCode() {
	var chartType = $($(".current-chart")[0]).attr("for");
	var offset = Number($("#searchOffset").val()) - 1;
	var limit = (Number($("#searchEnd").val()) - offset);
	var iframeURL = window.location.href
			+ '?iframe=true&charttype=' + chartType
			+ '&offset=' + offset
			+ '&limit=' + limit;

	if (currentSortCol != null) {
		iframeURL = iframeURL 
				+ '&currentsortcol=' + window.encodeURI(JSON.stringify(currentSortCol))
				+ '&isasc=' + isAsc;
	};

	if (lastQuery != undefined) {
		if (lastQuery === "search") {
			iframeURL = iframeURL + '&searchq=' + $("#searchQ").val();
		} else if (lastQuery === "filter") {
			var filtersArr = getFilters();
			if (filtersArr.length > 0) {
				filterFieldsStr = '';
				$.each(gfields, function(index, field){
					if (field.id != "_full_count" && field.id != "rank" && field.id != "_full_text") {
						filterFieldsStr = filterFieldsStr + '"' + field.id + '",';
					}
				});
				iframeURL = iframeURL
						+ '&filters=' + window.encodeURI(JSON.stringify(filtersArr))
						+ '&gfields=' + window.encodeURI(JSON.stringify(gfields));
			}
		};
	};

	if (chartType != "pie" && chartType != "pie3d") {
		var syList = new Array();
		var sy;
		var advancedGraph = false;
		$.each($("#series").find("fieldset"), function(index, series) {
			if (index != 0) {
				sy = new Object();
				sy.type = $(this).find("select[name='series-type']")[0].value;
				sy.name = $(this).find("select[name='series-field']")[0].value;
				syList.push(sy);
			}
		});
		iframeURL = iframeURL
				+ '&groupcolumntype=' + $("#group-column-type").val()
				+ '&groupcolumn=' + $("#group-column").val()
				+ '&fields=' + window.encodeURI(JSON.stringify(syList));
	} else {
		iframeURL = iframeURL + '&groupbyfield=' + $("#group-by-field").val();
	}

	var embedCode = '<iframe src="' + iframeURL + '" frameborder="0" data-module="data-viewer" style="overflow: hidden; height: 400px;width: 628px;"></iframe>';

	switch($("#embed-type").val()){
		case "iframe":
			break;

		case "html":
			embedCode = '<!DOCTYPE html>\n' +
						'<html lang="en">\n' +
						'<head>\n' +
						'	<meta charset="UTF-8">\n' +
						'	<title>' + resourceId + ' data preview</title>\n' +
						'</head>\n' +
						'<body>\n' +
						'	<div>\n\t\t' +
						embedCode + '\n' +
						'	</div>\n' +
						'</body>\n' +
						'</html>';
			break;

		case "js":
			embedCode = '<script>\n' +
						'	document.addEventListener("DOMContentLoaded", function(event) {\n' +
						'		var dataPreview = document.getElementById("data_preview");\n' +
						'		dataPreview.innerHTML = dataPreview.innerHTML + \'' + embedCode + '\';\n' +
						'	});\n' +
						'</script>';
			break;
	}

	$("#embedCode").val(embedCode);
}

function processData(data) {
	var fields = data.result.fields;
	var records = data.result.records;
	var fieldID;
	$.each(fields, function(index, field) {
		fieldID = field.id;
		if(field.type != "int4" && field.type != "numeric" && field.type != "timestamp") {
			var pureNull = true;
			var hasNull = false;
			var isInt = true;
			var isYMDDate = true;
			var isDMYDate = true;
			var isMDYDate = true;

			$.each(records, function(rindex, record) {
				var temp = record[fieldID];
				if (temp === "") {
					hasNull = true;
				} else {
					pureNull = false;
					if (isNaN(temp)) {
						isInt = false;
						if (isYMDDate && !checkYMDDate(temp)) {
							isYMDDate = false;
						};
						if (isDMYDate && !checkDMYDate(temp)) {
							isDMYDate = false;
						};
						if (isMDYDate && !checkMDYDate(temp)) {
							isMDYDate = false;
						};
					};
				}

				if (!pureNull && !isInt && !isYMDDate && !isDMYDate && !isMDYDate) {
					return false;
				}
			});
			if (pureNull) {
				fields[index].type = "purenull";
			} else if (isInt) {
				fields[index].type = "number";
			} else if (isYMDDate) {
				fields[index].type = "date";
				fields[index].format = "yyyy mm dd";
			} else if (isDMYDate) {
				fields[index].type = "date";
				fields[index].format = "dd mm yyyy";
			} else if (isMDYDate) {
				fields[index].type = "date";
				fields[index].format = "mm dd yyyy";
			}
		}
	});

	grecords = records;
	if (grecords.length > 0 || init) {
		gfields = fields;
	}
	if (data.result.total != undefined) {
		$("#doc-count").html(data.result.total);
	} else {
		if (data.result.sql === undefined) {
			$("#doc-count").html(0);
		} else {
			$.post(el.endPoint
					+ "api/3/action/datastore_search_sql?sql=SELECT COUNT(*) AS total_count FROM ("
					+ getFilterSQL(false) + ") AS filter_table",
			function(data) {
				$("#doc-count").html(
						data.result.records[0]["total_count"]);
			}).done();
		}
	}

	if (records != null) {
		var fieldOptionHTML = "";
		var fieldID;
		var fieldType;
		$.each(fields, function(index, field) {
			fieldID = field.id;
			fieldType = field.type;

			if (fieldID != "_id" && fieldID != "_full_count" && fieldID != "rank") {
				fieldOptionHTML = fieldOptionHTML + "<option value='" + fieldID + "'>" + fieldID + "</option>";
			};
		});
		// resourcePrivate?$(".filter-panel").html('<div style="margin-top: 200px;text-align: center;"><span style="font-size: 16px;">Sorry,due to API limitation,private dataset can not use filters.</span></div>'):$("#filter-field").html("<option disabled='disabled' selected='selected'>No column selected</option>" + fieldOptionHTML);
		$("#filter-field").html("<option disabled='disabled' selected='selected'>No column selected</option>" + fieldOptionHTML);
		var groupColumn = $("#group-column").val();
		$("#group-column").html("<option selected='selected' value=''>No column selected</option><option value='_id'>_id</option>" + fieldOptionHTML).val(groupColumn);
		var groupByField = $("#group-by-field").val();
		$("#group-by-field").html(fieldOptionHTML);
		if (groupByField != null) {
			$("#group-by-field").val(groupByField);
		}
	}
	$(".loading-dialog").hide();
	$(".recline-data-explorer").show();
	if (init) {
		addSeries();
		map = new L.map('leaflet');

	} else {
		reDrawGraph();
	}

	if (map != null) {
		map.remove();
		map = new L.map('leaflet');
		initMap = true;
	}

	if (el.leaflet.css("display") === "block") {
		createMap();
	}
	if (!iframeModel && currentSortCol != null) {
		grid.invalidate();
		grid.render();
		$(".alert-messages").hide();
	} else {
		createGrid();
	}
	$("#filter-field-type").attr("disabled", "disabled");
	$("#add-filter-btn").attr("disabled", "disabled");
}

function checkYMDDate(str) {
	var regex = /^(1|2\d{3}(-|\/)([1-9]|(0[1-9])|(1[0-2]))(-|\/)([1-9]|(0[1-9])|([1-2][0-9])|(3([0|1]))))((\s|T)([0-9]|((0|1)[0-9])|(2[0-3])):([1-9]|([0-5][0-9])):([1-9]|([0-5][0-9])))?(\.\d{3})?Z?$/
	return regex.test(str);
}

function checkDMYDate(str) {
	var regex = /^(([1-9]|(0[1-9])|([1-2][0-9])|(3([0|1])))(-|\/)([1-9]|(0[1-9])|(1[0-2]))(-|\/)(1|2\d{3}))((\s|T)([0-9]|((0|1)[0-9])|(2[0-3])):([1-9]|([0-5][0-9])):([1-9]|([0-5][0-9])))?(\.\d{3})?Z?$/
	return regex.test(str);
}

function checkMDYDate(str) {
	var regex = /^(([1-9]|(0[1-9])|(1[0-2]))(-|\/)([1-9]|(0[1-9])|([1-2][0-9])|(3([0|1])))(-|\/)(1|2\d{3}))((\s|T)([0-9]|((0|1)[0-9])|(2[0-3])):([1-9]|([0-5][0-9])):([1-9]|([0-5][0-9])))?(\.\d{3})?Z?$/
	return regex.test(str);
}

function checkURL(str) {
	//var regex = /^((https|http|ftp|rtsp|mms)?:\/\/)?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?(([0-9]{1,3}\.){3}[0-9]{1,3}|([0-9a-z_!~*'()-]+\.)*([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\.[a-z]{2,6})(:[0-9]{1,4})?((\/?)|(\/[0-9a-z_!~*'().;?:@&=+$,%#-]+)+\/?)$/
	var regex = /^(http|https|ftp):\/\//;
	return regex.test(str);
}

function createGrid() {
	var columnpicker;

	var columns = new Array();
	var column;

	$.each(gfields, function(index, field) {
		fieldID = field.id;
		if (fieldID != "rank" && fieldID != "_full_count" && fieldID != "_full_text") {
			column = new Object();
			column.id = fieldID;
			column.name = fieldID;
			column.field = fieldID;
			column.sortable = true;
			column.formatter = function(row, cell, value, columnDef, dataContext) {
                if(checkURL(value)) {
                    return "<a href='"+value+"' target='_blank' style='color: #002c76; padding-left: 4px;'>" + value + "</a>"
                }
                return value;
			}
			columns.push(column);
		}
	});

	var options = {
		enableCellNavigation : true,
		enableColumnReorder : false
	};

	function getItem(index) {
		return grecords[index];
	}

	function getLength() {
		return grecords.length;
	}

	grid = new Slick.Grid("#slickgrid", {getLength:getLength,getItem:getItem}, columns, options);
	columnpicker = new Slick.Controls.ColumnPicker(columns, grid, options);

	grid.onSort.subscribe(function(e, args) {
		currentSortCol = args.sortCol;
		isAsc = args.sortAsc;
		// if ($("#searchQ").val() != "" || resourcePrivate) {
		if ($("#searchQ").val() != "") {
			search();
		} else {
			filter();
		}
		$(".alert-messages").show();
	});

	if (el.sclickGrid.css("overflow") === "hidden") {
		el.sclickGrid.css("overflow", "");
	}
}

function createMap() {
	var hasAddress = false;
	var addressField;
	var geocode = false, latField, longField, latType, longType;

	initMap = false;

	$.each(gfields, function(index, field) {
		if (field.id.toLowerCase() === "lon") {
			longField = field.id;
			longType = getFieldType(field.id);
		};
		if (field.id.toLowerCase() === "lat") {
			latField = field.id;
			latType = getFieldType(field.id);
		}
		if (latType != undefined
			&& longType != undefined
			&& (latType === "number" || latType === "int4" || latType === "numeric") 
			&& (longType === "number" || longType === "int4" || longType === "numeric")) {
			hasAddress = false;
			geocode = true;
			return false;
		};
		if (field.id.toLowerCase() === "address") {
			hasAddress = true;
			addressField = field.id;
		}
	});

	// map = new L.map('leaflet');
	L.tileLayer('http://otile{s}.mqcdn.com/tiles/1.0.0/osm/{z}/{x}/{y}.png',
	{
		attribution : 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
		maxZoom : 18,
		subdomains : '1234'
	}).addTo(map);

	// *** set bounderies start
	var org_id = $("#data-organization-id", window.parent.document).attr("data-organization-id");
	if (org_id.length > 0) {
		$.ajax({
			url : el.endPoint + "/geometry/organization/" + org_id,
			type : 'get',
			success : function(data) {
				if (data.success) {
					var states = [ {
						"type" : "Feature",
						"properties" : {},
						"geometry" : $.parseJSON(data.result.spatial)
					} ];
					featureGroup = L.geoJson(states, {
						style : function(feature) {
							return {
							    stroke: false,
							    weight: 0,
							    color: '#fff',
							    fillOpacity: 0,
								opacity : 0
							}
						}
					}).addTo(map);
					fitMapBounds();
					noBoundery = false;
				}
			}
		});
	}

	// *** set bounderies end
	
	featureGroup = new L.featureGroup().addTo(map);

	if (geocode) {
		$.each(grecords, function(index, record){
			if (checkUSGeocode(record)) {
				featureGroup.addLayer(createMarker(record[latField], record[longField], record));
			};
		});
		if (noBoundery) {
            fitMapBounds();
        }
        $(".alert-messages").css("display", "none");
        
	} else if (grecords.length > 0 && hasAddress) {
		$(".alert-messages").show();
		// featureGroup = new L.featureGroup().addTo(map);

		require(["esri/tasks/locator"],function(Locator){
			var locator, feature, addrFieldNotNullCount = 0, arcgisResponseConut = 0;

            $.each(grecords, function(index, record) {
                try {
                    locator = new Locator("http://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer");//?sourceCountry=USA");
                    locator.on("address-to-locations-complete", showResults);

                    if (record[addressField] != "") {
                        addrFieldNotNullCount++;
                        locator.outSpatialReference = map.spatialReference;
                        locator.addressToLocations({
                            "address" : {
                                "SingleLine" : record[addressField],
                                "maxLocations" : 1
                            }
                        });
                    }
                } catch (e) {
                } finally {
                    return true;
                }

                function showResults(data) {
                    arcgisResponseConut++;
                    if (data['addresses'].length > 0) {
                        var location = data['addresses'][0]['location'];
                        featureGroup.addLayer(createMarker(location['y'], location['x'], record));
                        if (arcgisResponseConut === addrFieldNotNullCount) {
                            if (noBoundery) {
                                fitMapBounds();
                            }
                            $(".alert-messages").css("display", "none");
                        }
                    }
                }
            });
		});
	} else {
		if (noBoundery) {
			fitMapBounds();
		}
	}

	function checkUSGeocode(record){
            var lat = record[latField], lon = record[longField];
            return ( (lat != '' && lat != null) && (lon != '' && lon != null)) ? true: false;
	}

	function createMarker(lat, lng, record) {
		var marker = new L.marker([lat,lng]).addTo(map);

        marker.on('mouseover', function() {
            marker.bindPopup(createPopupContent(record)).openPopup();
            var leafletPopup =  $(".leaflet-popup.leaflet-zoom-animated");
            leafletPopup.css({"bottom": "50px", "position": "relation", "z-index": 10000});
            var leftWidth = leafletPopup.width() / 2 - 20;
            leafletPopup.append('<div class="leaflet-popup-tip-container" style="background:#ccc; filter:alpha(opacity=0); -moz-opacity:0;-khtml-opacity:0;opacity:0;"></div>');
            leafletPopup.find(".leaflet-popup-tip-container").css({"padding-bottom":"30px", "position": "absolute", "left": leftWidth + "px"});
            leafletPopup.bind("mouseleave", function(e) {
                marker.closePopup();
            });
        });
        return marker;
	}

	function createPopupContent(record) {
		var html = '', menus = [];
	    $("#columns-form").find("input[name=menus]:checked").each(function() {
	        menus.push($(this).val());
	    });

	    if(menus.length > 0) {
	        for (var i=0; i < menus.length; i++){
	            var value = record[menus[i]];
	            if (checkURL(value)) {
	                html += '<div><strong>' + menus[i] + '</strong>: <a href="' + value + '" target="_blank">'+ value + '</a></div>';
	            } else {
	                html += '<div><strong>' + menus[i] + '</strong>: '+ value + '</div>';
	            };
	        }
	    } else {
	        html += "This popup has no description.";
	    }
		return html;
	}
}

function fitMapBounds() {
	if (featureGroup != null) {
		var bounds = featureGroup.getBounds();
		if (bounds && bounds.getNorthEast() && bounds.getSouthWest()) {
			map.invalidateSize();
			map.fitBounds(bounds);
		} else {
			map.setView([ 0, 0 ], 2);
		}
	}
}

function getCategoriesAndData(seriesX, seriesY) {
	var cd = new Object();
	var categories = new Array();
	var seriesList = new Array();
	var series;
	var dataY;
	$.each(seriesY, function(index, sy) {
		series = new Object();
		series.name = sy;
		series.data = new Array();
		seriesList.push(series);
	});

	$.each(graphRecords!=null?graphRecords:grecords,function(index, record){
		if (seriesX != "" && seriesX != null) {
			categories.push(record[seriesX]);
		} else {
			categories.push(record["_id"]);
		}
		;
		$.each(seriesList, function(index, sy) {
			var dataY = record[sy.name];
			if (dataY === "") {
				sy.data.push(null);
			} else {
				sy.data.push(Number(dataY));
			}
		});
	});

	cd.categories = categories;
	cd.series = seriesList;
	return cd;
}

function createLineChart(seriesX, seriesY) {
	var cd = getCategoriesAndData(seriesX, seriesY);
	var hgOptions = {
		title : {
			text : ''
		},
		yAxis : {
			title : {
				text : ""
			},
			plotLines : [ {
				value : 0,
				width : 2,
				color : '#808080'
			} ]
		},
		legend : {
			layout : 'vertical',
			align : 'right',
			verticalAlign : 'middle',
			borderWidth : 0
		},
		series : cd.series
	};

	if (seriesX != "" && seriesX != null) {
		hgOptions.xAxis = {
			categories : cd.categories
		}
	} else {
		hgOptions.xAxis = {
			categories : cd.categories,
			labels : {
				enabled : false
			}
		}
	}
	$('#highcharts').highcharts(hgOptions);
}

function createAreasplineChart(seriesX, seriesY) {
	var cd = getCategoriesAndData(seriesX, seriesY);
	var hgOptions = {
		chart : {
			type : 'areaspline'
		},
		title : {
			text : ''
		},
		legend : {
			layout : 'vertical',
			align : 'left',
			verticalAlign : 'top',
			x : 150,
			y : 100,
			floating : true,
			borderWidth : 1,
			backgroundColor : '#FFFFFF'
		},
		yAxis : {
			title : {
				text : ""
			}
		},
		tooltip : {
			shared : true
		},
		credits : {
			enabled : false
		},
		plotOptions : {
			areaspline : {
				fillOpacity : 0.5
			}
		},
		series : cd.series
	};

	if (seriesX != "" && seriesX != null) {
		hgOptions.xAxis = {
			categories : cd.categories
		}
	} else {
		hgOptions.xAxis = {
			categories : cd.categories,
			labels : {
				enabled : false
			}
		}
	}
	$('#highcharts').highcharts(hgOptions);
}

function createColumnChart(seriesX, seriesY) {
    var cd = getCategoriesAndData(seriesX, seriesY);
    var hgOptions = {
        chart: {
            type: 'column'
        },
        plotOptions : {
            column : {
                pointPadding : 0.2,
                borderWidth : 0,
                shadow: true
            }
        },
        title : {
            text : ''
        },
        yAxis : {
            min : 0,
            title : {
                text : ""
            }
        },
        tooltip : {
            headerFormat : '<span style="font-size:10px">{point.key}</span><table>',
            pointFormat : '<tr><td style="color:{series.color};padding:0">{series.name}: </td>'
                    + '<td style="padding:0"><b>{point.y:.1f}</b></td></tr>',
            footerFormat : '</table>',
            shared : true,
            useHTML : true
        },
        series: cd.series
    };

    if (seriesX != "" && seriesX != null) {
        hgOptions.xAxis = {
            categories : cd.categories
        }
    } else {
        hgOptions.xAxis = {
            categories : cd.categories,
            labels : {
                enabled : false
            }
        }
    }

    $('#highcharts').highcharts(hgOptions);
}

function create3DColumnChart(seriesX, seriesY) {
	$("#sliders").show();
	var cd = getCategoriesAndData(seriesX, seriesY);
	var hgOptions = {
		chart : {
			renderTo : 'highcharts',
			type : 'column',
			margin : 75,
			options3d : {
				enabled : true,
				alpha : 15,
				beta : 15,
				depth : 50,
				viewDistance : 25
			}
		},
		title : {
			text : ''
		},
		plotOptions : {
			column : {
				depth : 25
			}
		},
		series : cd.series
	};

	if (seriesX != "" && seriesX != null) {
		hgOptions.xAxis = {
			categories : cd.categories
		}
	} else {
		hgOptions.xAxis = {
			categories : cd.categories,
			labels : {
				enabled : false
			}
		}
	}

	chart = new Highcharts.Chart(hgOptions);
	$('#R0').on('change', function() {
		chart.options.chart.options3d.alpha = this.value;
		showValues();
		chart.redraw(false);
	});
	$('#R1').on('change', function() {
		chart.options.chart.options3d.beta = this.value;
		showValues();
		chart.redraw(false);
	});

	function showValues() {
		$('#R0-value').html(chart.options.chart.options3d.alpha);
		$('#R1-value').html(chart.options.chart.options3d.beta);
	}
	showValues();
}

function createBar(seriesX, seriesY) {
	var cd = getCategoriesAndData(seriesX, seriesY);

	var hgOptions = {
		chart : {
			type : 'bar'
		},
		yAxis : {
			title : {
				text : null,
				align : 'high'
			},
			labels : {
				overflow : 'justify'
			}
		},
		plotOptions : {
			bar : {
				dataLabels : {
					enabled : true
				}
			}
		},
		legend : {
			layout : 'vertical',
			align : 'right',
			verticalAlign : 'top',
			x : -40,
			y : 100,
			floating : true,
			borderWidth : 1,
			backgroundColor : '#FFFFFF',
			shadow : true
		},
		credits : {
			enabled : false
		},
		series : cd.series
	};

	if (seriesX != "" && seriesX != null) {
		hgOptions.xAxis = {
			categories : cd.categories
		}
	} else {
		hgOptions.xAxis = {
			categories : cd.categories,
			labels : {
				enabled : false
			}
		}
	}

	$('#highcharts').highcharts(hgOptions);
}

function createPie(is3D) {
	var groupByField = iframeModel ? getUrlParam("groupbyfield") : $("#group-by-field").val();
	var offset = iframeModel ? Number(getUrlParam("offset")) : Number($("#searchOffset").val());
	var limit = iframeModel ? Number(getUrlParam("limit")) : Number($("#searchEnd").val()) - offset;
	if (limit <= 0) {
		limit = 100;
		$("#searchEnd").val(offset + 100);
	}

	var series = new Array();
	var seriesObj = new Object();
	var seriesData = new Array();
	var seriesSingleData;
	var fieldValue;

	$.each(grecords, function(index, record) {
		fieldValue = record[groupByField];

		if (fieldValue === "") {
			fieldValue = "null";
		}
		seriesSingleData = null;
		$.each(seriesData, function(index, sd) {
			if (sd[0] === fieldValue) {
				seriesSingleData = sd;
				return false;
			};
		});

		if (seriesSingleData === null) {
			seriesSingleData = new Array();
			seriesSingleData.push(fieldValue);
			seriesSingleData.push(1);
			seriesData.push(seriesSingleData);
		} else {
			seriesSingleData[1]++;
		}
	});

	seriesObj.type = "pie";
	seriesObj.name = groupByField;
	seriesObj.data = seriesData;
	series.push(seriesObj);
	var hgOptions = {
		title : {
			text : 'Group By ' + groupByField
		},
		tooltip : {
			pointFormat : '{series.name}: <b>{point.percentage:.1f}%</b>'
		},
		plotOptions : {
			pie : {
				allowPointSelect : true,
				cursor : 'pointer',
				dataLabels : {
					enabled : true,
					color : '#000000',
					connectorColor : '#000000',
					format : '<b>{point.name}</b>: {point.percentage:.1f} %'
				}
			}
		},
		series : series
	};

	if (is3D) {
		hgOptions.chart = {
			type : 'pie',
			options3d : {
				enabled : true,
				alpha : 45,
				beta : 0
			}
		};
		hgOptions.plotOptions.pie.depth = 35;
	} else {
		hgOptions.chart = {
			plotBackgroundColor : null,
			plotBorderWidth : null,
			plotShadow : false
		};
	}

	$('#highcharts').highcharts(hgOptions);
}

function create3DPie(){
	createPie(true);
}

function sqlReplace(str) {
	 return escapeRule(str.replace(/\'/g, '\"'));
 }

function valReplace(str) {
	return escapeRule(str.replace(/\'/g, '\\\''));
 }

 function escapeRule(str) {
	return str.replace(/\+/g, '%2B')
	 .replace(/\?/g, '%3F')
	 .replace(/\//g, '%2F')
	 .replace(/\&/g, '%26')
	 .replace(/\#/g, '%23')
	 .replace(/\!/g, '%21')
	 .replace(/\|/g, '%7C')
	 .replace(/\=/g, '%3D')
	 .replace(/\^/g, '%5E')
	 .replace(/\`/g, '%60')
	 .replace(/\{/g, '%7B')
	 .replace(/\}/g, '%7D')
	 .replace(/\]/g, '%5D')
	 .replace(/\[/g, '%5B')
	 .replace(/\"/g, '%22')
	 .replace(/\</g, '%3C')
	 .replace(/\>/g, '%3E');
 }

