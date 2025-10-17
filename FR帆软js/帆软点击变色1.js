// 复选框状态改变事件 - 优先级高
// 检查当前是否为sheet2
// 输出当前工作表名称用于调试
//console.log('当前工作表名称:', _g().currentSheetName);
//var currentSheetName = _g().currentSheetName;
//if (currentSheetName !== 'sheet2') {
//    // 如果不是sheet2，则不执行后续代码
//    return;
//}

var checked = this.getValue();
var location = this.options.location;
var rowIndex = FR.cellStr2ColumnRow(location).row;

var $rowElement = $('tr[tridx="' + rowIndex + '"]');

if ($rowElement.length > 0) {
    if (checked) {
        // 复选框选中 - 设置复选框选中标记和颜色
        $rowElement.attr('data-checkbox-selected', 'true');
        $rowElement.attr('data-click-selected', 'false'); // 清除点击选中状态
        $rowElement.css('background-color', '#e8f5e9');
        $rowElement.find('td').css('background-color', '#e8f5e9');
    } else {
        // 复选框取消选中 - 清除复选框选中标记和颜色
        $rowElement.attr('data-checkbox-selected', 'false');
        $rowElement.css('background-color', '');
        $rowElement.find('td').css('background-color', '');
        
        // 如果之前有点击选中，恢复点击选中颜色
        var isClickSelected = $rowElement.attr('data-click-selected') === 'true';
        if (isClickSelected) {
            $rowElement.css('background-color', '#e8f5e9');
            $rowElement.find('td').css('background-color', '#e8f5e9');
        }
    }
}
;


// 行点击变色，再次点击恢复原色，只影响当前行
$(document).ready(function() {
    $(".x-table").on('click', 'tr', function (event) {
        var $row = $(this);
        // 新增S列判断（假设S列是第19列，索引18）
        var sValue = $row.find("td:eq(18)").text().trim();
        if(sValue !== "1") return; // 如果S列值为"1"，则不执行点击变色
        var $clickedRow = $(this);
        var $target = $(event.target);
        var checkboxColumnIndex = 20; // U列的索引（从0开始）
        var clickedCellIndex = $target.closest('td').index();

        // 排除复选框列和控件
        if (clickedCellIndex === checkboxColumnIndex) return;
        if ($target.is('input[type="checkbox"]') || $target.closest('input[type="checkbox"]').length > 0) return;

        // 跳过表头
        if ($clickedRow.closest('thead').length > 0) return;

        // 跳过复选框已选中行
        if ($clickedRow.attr('data-checkbox-selected') === 'true') return;

        var isClickHighlighted = $clickedRow.attr('data-click-selected') === 'true';
        if (isClickHighlighted) {
            $clickedRow.attr('data-click-selected', 'false');
            $clickedRow.css('background-color', '');
        } else {
            $clickedRow.attr('data-click-selected', 'true');
            $clickedRow.css('background-color', '#EAF6F6');
        }
    });
});



	// 获取S4单元格值
	var checkValue = _g().getCellValue("S4");  
	// 获取目标控件（假设控件位于A5单元格）
	var widget = _g().getWidgetByCell("U4");  
	 
	// 判断并设置控件状态
	if(checkValue == 1) {
	    widget.setEnable(false);  // 不可用
	} else {
	    widget.setEnable(true);   // 恢复可用
	}
;
    


// 复选框状态改变事件 - 优先级高
// 检查当前是否为sheet2
// 输出当前工作表名称用于调试
//console.log('当前工作表名称:', _g().currentSheetName);
//var currentSheetName = _g().currentSheetName;
//if (currentSheetName !== 'sheet2') {
//    // 如果不是sheet2，则不执行后续代码
//    return;
//}

var checked = this.getValue();
var location = this.options.location;
var rowIndex = FR.cellStr2ColumnRow(location).row;

var $rowElement = $('tr[tridx="' + rowIndex + '"]');

if ($rowElement.length > 0) {
    if (checked) {
        // 复选框选中 - 设置复选框选中标记和颜色
        $rowElement.attr('data-checkbox-selected', 'true');
        $rowElement.attr('data-click-selected', 'false'); // 清除点击选中状态
        $rowElement.css('background-color', '#e8f5e9');
        $rowElement.find('td').css('background-color', '#e8f5e9');
    } else {
        // 复选框取消选中 - 清除复选框选中标记和颜色
        $rowElement.attr('data-checkbox-selected', 'false');
        $rowElement.css('background-color', '');
        $rowElement.find('td').css('background-color', '');
        
        // 如果之前有点击选中，恢复点击选中颜色
        var isClickSelected = $rowElement.attr('data-click-selected') === 'true';
        if (isClickSelected) {
            $rowElement.css('background-color', '#e8f5e9');
            $rowElement.find('td').css('background-color', '#e8f5e9');
        }
    }
}
;


	// 示例：假设要校验C列的值是否等于"禁止提交"
	var currentRow = FR.cellStr2ColumnRow(this.options.location).row;
	var checkValue = _g().getCellValue(1, 18, currentRow); // 获取C列当前行值（参数说明：0表示第一个sheet，2表示第3列，从0开始索引）
	var checkValue_t = _g().getCellValue(1, 21, currentRow); // 获取C列当前行值（参数说明：0表示第一个sheet，2表示第3列，从0开始索引） 
	if (checkValue == "1" && checkValue_t != "0") {
	    FR.Msg.alert("提交失败", "目标列包含禁止提交的值");
	    return false; // 终止提交动作
	} else {
	    // 触发真实的提交操作（需另有一个隐藏的提交按钮）
	    _g().getWidgetByName("submitButton").fireEvent("click"); 
	}