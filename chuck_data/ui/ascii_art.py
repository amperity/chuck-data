"""
ASCII art and styling for Chuck TUI.
"""

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.box import ROUNDED

from chuck_data.ui.theme import CHUCK_LOGO, DIALOG_BORDER, MESSAGE_LIGHT

# ASCII art for Chuck logo

CHUCK_LOGO_ART = r"""
                               .:::::::::
                             -----------=++
                           -***::::--::----:*#=
                         .**- %%%+:::+:---::#@@@
                        .:*+:.%++++==+:::==+#-+@
                        :#-.:===============*#%%                    
                      -:%:  .@@-        :@@=                        
                    :-:+=:-: ==%    +++. ==%    +#*                 
                      ::::=+=      +++++-     .+++                   
                    :--::+++++++@@@%   +@@@++++++%:...                
                   :+#*::=++++=@@@@@@ @@@@@@*+++%%%%-                  
                  :::::#::=*#+=-@@@.. ..-@@-++*#%@                      
                  ..:=*:::=*%%%*----@@@*---+#%%%%
                   +=::::--+%%%%###++++++##%%%%%@
                 :+::::::=++#%%%%%%%%%%%%%%%%#*+@
                #:::::::-++++%%%%%%@@@@@@%%%%*++%@                   
              .=-:::::::-++++*%%%@@@@@@@@@%%+++++@                   
              -*::::::::--=+++#%%@@@@@@@@@%#==+++@                   
             =+::------::--=++--#%%@@@@@@%%-++++%@                   
            *=:::---=+++-::--=+::+%%@@@%#+:=+++*%                      
            *=::---=++++++-::-    .%%%%:    =+#@                        
            *=::---=+++++#%#=    :+@@@@#-    =-                          
            *=::----=++++#%%%%  @@@@@@@@@@  --.
            **-:::----===:*%%@@@@@@@@@@@@@%-=*=
          ++-*+:::-------:*%%@@@@@@@@@@@@#--+#=
      :==--:::=#::::::::::*%%%@@@@@@@@%%*=--++-
     ==----::::-*:::::::::*%%%%@@@@@@%#-:::--
    #-::::::*#=   :        -%%%%%%%%%
    :------          ......:-------  .........

      ________  ___  ___  ___  ___  ________  ___  __
     |\   ____\|\  \|\  \|\  \|\  \|\   ____\|\  \|\  \
     \ \  \___|\ \  \\\  \ \  \\\  \ \  \___|\ \  \/  /|_       
      \ \  \    \ \   __  \ \  \\\  \ \  \    \ \   ___  \
       \ \  \____\ \  \ \  \ \  \\\  \ \  \____\ \  \\ \  \
        \ \_______\ \__\ \__\ \_______\ \_______\ \__\\ \__\
         \|_______|\|__|\|__|\|_______|\|_______|\|__| \|__|

       ________  ________  _________  ________
      |\   ___ \|\   __  \|\___   ___\\   __  \
      \ \  \_|\ \ \  \|\  \|___ \  \_\ \  \|\  \
       \ \  \ \\ \ \   __  \   \ \  \ \ \   __  \
        \ \  \_\\ \ \  \ \  \   \ \  \ \ \  \ \  \
         \ \_______\ \__\ \__\   \ \__\ \ \__\ \__\
          \|_______|\|__|\|__|    \|__|  \|__|\|__|
    
     
"""

# Welcome message
WELCOME_MESSAGE = r"""
Welcome to the Chuck Data agentic data engineering research preview!

Chuck is an agent for building amazing customer centric data models and workflows in your Databricks account.

Work with Chuck via natural language prompts or direct commands.

Check us out on discord by running /discord!

Try out /help or /tips to get started, or just ask Chuck a question!
"""


def display_welcome_screen(console: Console) -> None:
    """
    Display the welcome screen with ASCII art and welcome message.

    Args:
        console: Rich console instance
    """
    # Display the ASCII art logo with styling
    logo_text = Text(CHUCK_LOGO_ART)
    logo_text.stylize(CHUCK_LOGO)
    console.print(logo_text)

    # Display welcome message in a box
    welcome_panel = Panel(
        Text(WELCOME_MESSAGE, style=MESSAGE_LIGHT, justify="left"),
        box=ROUNDED,
        border_style=DIALOG_BORDER,
        padding=(1, 2),
        title=f"[bold {DIALOG_BORDER}]Chuck Data Research Preview[/bold {DIALOG_BORDER}]",
        title_align="left",
    )
    console.print(welcome_panel)

    # Add a little space after the welcome panel
    console.print()
