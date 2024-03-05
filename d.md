1. Onprem manda un asset con state Active
2. Enritcher federa con la key del state federado
3. En el cloud se cambia el state por otro
4. El ompre vuleve a mandar el asset
5. En el enritcher le asigna el anterior y no el actualizado por el usuario

Posible solucion
- Cuando en el coloud se edita un campo federado, se marca el asset como editado en el cloud y
  los cambios de onprem ya no reflejan cambios en entidades federadas
  - Controlar update inidividual por campo federado o para todos