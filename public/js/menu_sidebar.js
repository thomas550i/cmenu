$(document).ready(function() {
$('.layout-side-section').hide();
    frappe.call({
        method: "cmenu.api.get_user_menu",  // API method in your Python code
        callback: function(r) {
            if (r.message) {
                var menuTree = r.message;
                renderSidebarMenu(menuTree);
            }
        }
    });
  
  function renderSidebarMenu(menuTree) {
        // Create and append cmenu_sidebar div after the body tag
        var body = document.querySelector("body");
        var cmenuSidebar = document.createElement("div");
        cmenuSidebar.id = "cmenu_sidebar";
        body.appendChild(cmenuSidebar);
  
        // Build the main menu structure (ul with class="menu")
        var menuList = document.createElement("ul");
        menuList.className = "menu";  // Main menu class
  
        // Iterate over the parent menus in menuTree
        menuTree.forEach(function(parent) {
            var parentItem = document.createElement("li");
            parentItem.className = "list";  // Parent menu class
  
            // Create parent link (a) and append it to the parent item
            var parentLink = document.createElement("a");
            parentLink.href = parent.link;  // Link for the parent item
            

            var icon = document.createElement("i");
            icon.className = parent.icon
            icon.setAttribute("aria-hidden", "true");

            var PText = document.createElement("p");
            PText.textContent = parent.name;  // Parent menu name
            var span = document.createElement("span");
            span.className = "caret";  // Parent menu name
            
            parentLink.appendChild(icon)
            parentLink.appendChild(PText)
            if (parent.children && parent.children.length > 0) {
               parentLink.appendChild(span)
            }
            

            parentItem.appendChild(parentLink);
  
            // Check if the parent has children
            if (parent.children && parent.children.length > 0) {
                // Create child list (ul with class="items") for child items
                var childList = document.createElement("ul");
                childList.className = "items";  // Child list class
  
                parent.children.forEach(function(child) {
                    var childItem = document.createElement("li");
  
                    // Create child link (a) and append to child item
                    var childLink = document.createElement("a");
                    childLink.href = child.link;  // Child item link
                    

                    var cicon = document.createElement("i");
                    cicon.className = child.icon
                    cicon.setAttribute("aria-hidden", "true");

                    var cPText = document.createElement("p");
                     cPText.textContent = child.name;  // Parent menu name
                  

                     childLink.appendChild(cicon)
                     childLink.appendChild(cPText)
  
                    childItem.appendChild(childLink);
                    childList.appendChild(childItem);  // Append child item to child list
                });
  
                parentItem.appendChild(childList);  // Append child list to the parent item
            }
  
            menuList.appendChild(parentItem);   // Append parent item to the menu list
        });
  
        // Append the built menu list to cmenu_sidebar
        cmenuSidebar.appendChild(menuList);
    }
  
  
  });
  
  
  
  $(document).ready(function() {
    console.log("loading jquery");

    // When mouse enters the parent menu item, show the submenu
    $(document).on('mouseenter', '.list', function() {
        var parentLi = $(this); // Get the parent <li>
        parentLi.addClass('active'); // Add the active class to show submenu
    });

    // When mouse leaves the parent menu item, hide the submenu
    $(document).on('mouseleave', '.list', function() {
        var parentLi = $(this); // Get the parent <li>
        parentLi.removeClass('active'); // Remove the active class to hide submenu
    });
});