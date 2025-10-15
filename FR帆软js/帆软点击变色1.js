	// 监听控件状态变化
	var boxes = _g().getWidgetsByName("复选按钮控件名称");
	boxes.forEach(function(box) {
	    box.on("statechange", function() { //{{JS实现单选按钮选项不同颜色-4796.md}}
	        var location = this.options.location; // 获取控件所在单元格位置
	        var row = FR.cellStr2ColumnRow(location).row; // 转为行号
	        var isChecked = this.getValue(); // 获取勾选状态（true/false或1/0）
	        
	        // 根据状态设置行背景色
	        _g().setCellStyle(0, "A", row, "background:" + (isChecked ? "#90EE90" : ""));
	    });
	});

-- 点击变色
    // 针对帆软报表10.0优化的行点击变色功能，特别处理分组单元格
$(document).ready(function() {
    // 使用事件委托，处理动态加载的内容
    $(".x-table").on('click', 'tr', function() {
        var rowNum = $(this).index() + 1;
        if (rowNum < 2) return; // 过滤前3行（表头）
        
        var $clickedRow = $(this);
        var $tds = $clickedRow.find('td');
        
        // 检查当前行是否已经被高亮（检查第一个有背景色的td）
        var isHighlighted = false;
        $tds.each(function() {
            var bgColor = $(this).css('background-color');
            if (bgColor === 'rgb(230, 250, 246)' || bgColor === '#e6faf6' || 
                (bgColor && bgColor.indexOf('230') >= 0 && bgColor.indexOf('250') >= 0)) {
                isHighlighted = true;
                return false; // 跳出循环
            }
        });
        
        if (isHighlighted) {
            // 取消高亮
            $clickedRow.css('background-color', '');
            $tds.each(function() {
                $(this).css('background-color', '');
                // 处理分组单元格中的子元素
                $(this).find('*').each(function() {
                    var elemBg = $(this).css('background-color');
                    if (elemBg === 'rgb(230, 250, 246)' || elemBg === '#e6faf6' || 
                        (elemBg && elemBg.indexOf('230') >= 0 && elemBg.indexOf('250') >= 0)) {
                        $(this).css('background-color', '');
                    }
                });
            });
        } else {
            // 添加高亮
            $clickedRow.css('background-color', '#e6faf6');
            $tds.each(function() {
                $(this).css('background-color', '#e6faf6');
                // 特别处理分组单元格
                if ($(this).hasClass('x-group-cell') || $(this).find('.x-group-content').length > 0) {
                    $(this).find('*').each(function() {
                        $(this).css('background-color', '#e6faf6');
                    });
                }
            });
        }
    });
});