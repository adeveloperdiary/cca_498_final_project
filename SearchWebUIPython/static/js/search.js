$(document).ready(function () {


});


function search() {

    $("#search-results").html("");

    $.getJSON("/get_questions/" + $('#search-text').val(), function (data) {

        $('#hits').html(data.total);
        $('#time').html(data.time+' sec');

        var items = [];
        $.each(data.data, function (key, val) {
            items.push("<div class=\"card-body\" style='padding-bottom:0 !important;'>\n" +
                "                        <div class=\"card border-secondary\" style=\"border:1px solid #dfdfdf !important;border-radius:0;\">\n" +
                "                            <div class=\"card-header\" style=\"border-bottom-style: dashed;background-color: rgba(0, 0, 0, 0.03)\">\n" +
                "                                <div style=\"margin-right:20px;background-color: #0068b9;border-radius: 5px;padding:7px;\" class=\"float-left\">\n" +
                "                                    <div class=\"text-center\" style=\"font-size: 30px;font-weight: 600;color: #FFFFFF;\">" + val.count + "</div>\n" +
                "                                    <div style=\"color:#FFFFFF\">Answers</div>\n" +
                "                                </div>\n" +
                "                                <div style=\"color:#0078d5;font-weight: bold;margin-left:10px;font-size: 18px; line-height:100%;align-items: center;padding-bottom: 5px\">\n" +
                "                                    <a style=\"color:#0078d5;\" href=\"#\" data-id='" + key + "' class='question-title' onclick=\"show_answers('" + key + "','" + val.count + "')\">" + val.title + "</a>\n" +
                "                                </div>\n" +
                "                                <div>\n" +
                "                                    <label style=\"font-weight: bold;color:#666666;padding-right: 10px;padding-top:10px\">Score: </label><span>" + val.score + "</span>\n" +
                "                                    <label style=\"font-weight: bold;color:#666666;padding-right: 10px;padding-left:10px\">Date </label><span>" + val.date + "</span>\n" +
                "                                </div>\n" +
                "\n" +
                "                            </div>\n" +
                "                            <div class=\"card-body text-secondary\" style='font-size:16px;'>\n" +
                val.body +
                "                            </div>\n" +
                "                            <div class=\"card-footer text-muted\" style=\"border-top-style: dashed\">\n" +
                "                                <a href=\"#\" style=\"padding:5px\">python</a><a href=\"#\" style=\"padding:5px\">panda</a>\n" +
                "                            </div>\n" +
                "                        </div>\n" +
                "                    </div>");
        });

        $("<div/>", {
            "id": "search-results",
            html: items.join("")
        }).appendTo("#search-results");

        $('pre code').each(function (i, block) {
            hljs.highlightBlock(block);
        });

    });
}

function show_answers(key, count) {

    window.open('/static/results.html?id=' + key + '&c=' + count, '_self');

}